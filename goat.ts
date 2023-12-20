import { KeyValue, RequestHeader, Rpc } from "./gen/goatorepo/rpc_pb";
import { Message, AnyMessage, ServiceType, MethodInfo, PartialMessage, MethodKind } from "@bufbuild/protobuf";
import { ContextValues, createContextValues, Transport, StreamResponse, UnaryRequest, UnaryResponse, ConnectError, StreamRequest } from "@connectrpc/connect";
import { runUnaryCall, runStreamingCall, createMethodSerializationLookup, createWritableIterable, pipe } from "@connectrpc/connect/protocol";

export interface RpcReadWriter {
    read(): Promise<Rpc>;
    write(rpc: Rpc): Promise<void>;
}

export class GoatTransport implements Transport {
    channel: RpcReadWriter;
    outstanding = new Map<number, { resolve: (rpc: Rpc) => void; reject: (reason: any) => void; }>();
    // GOAT uses 64-bit ints for IDs natively, but it's fine to use
    // "number" as we're in charge of allocating them here, and there are enough
    // integers available to not overflow (Number.MAX_SAFE_INTEGER ~= 2^53)
    nextId: number = 0;
    readError: any = undefined;

    constructor(ch: RpcReadWriter) {
        this.channel = ch;

        this.startReader();
    }

    private startReader(): void {
        this.channel.read()
            .then(rpc => {
                const id = Number(rpc.id);

                const resolver = this.outstanding.get(id);
                if (resolver) {
                    resolver.resolve(rpc);
                }

                this.startReader();
            })
            .catch(reason => {
                this.readError = reason;

                for (const value of this.outstanding.values()) {
                    value.reject(reason);
                }
            });
    }

    unary<I extends Message<I> = AnyMessage, O extends Message<O> = AnyMessage>(
        service: ServiceType,
        method: MethodInfo<I, O>,
        signal: AbortSignal | undefined,
        timeoutMs: number | undefined,
        header: HeadersInit | undefined,
        input: PartialMessage<I>,
        contextValues?: ContextValues | undefined,
    ): Promise<UnaryResponse<I, O>> {
        if (this.readError) {
            throw (new Error(this.readError));
        }

        return runUnaryCall({
            interceptors: [],
            signal: signal,
            timeoutMs: timeoutMs,
            req: {
                stream: false,
                service: service,
                method: method,
                header: new Headers(header),
                contextValues: contextValues ?? createContextValues(),
                url: "",
                init: {},
                message: input,
            },
            next: req => {
                return this.performUnary(req);
            },
        });
    }

    stream<I extends Message<I> = AnyMessage, O extends Message<O> = AnyMessage>(
        service: ServiceType,
        method: MethodInfo<I, O>,
        signal: AbortSignal | undefined,
        timeoutMs: number | undefined,
        header: HeadersInit | undefined,
        input: AsyncIterable<PartialMessage<I>>,
        contextValues?: ContextValues | undefined,
    ): Promise<StreamResponse<I, O>> {
        if (this.readError) {
            throw (new Error(this.readError));
        }

        return runStreamingCall({
            interceptors: [],
            signal: signal,
            timeoutMs: timeoutMs,
            req: {
                stream: true,
                service: service,
                method: method,
                header: new Headers(header),
                contextValues: contextValues ?? createContextValues(),
                url: "",
                init: {},
                message: input,
            },
            next: req => {
                return this.performStreaming(req);
            },
        });
    }

    private async performUnary<I extends Message<I> = AnyMessage, O extends Message<O> = AnyMessage>(req: UnaryRequest<I, O>): Promise<UnaryResponse<I, O>> {
        const serdes = createMethodSerializationLookup(req.method, undefined, undefined, { writeMaxBytes: 10000000, readMaxBytes: 10000000 });
        const id = this.nextId++;
        const { promise: rpcPromise, resolve: rpcResolve, reject: rpcReject } = promiseWithResolvers<Rpc>();
        var ret: Rpc;

        req.signal.throwIfAborted();
        req.signal.addEventListener("abort", () => {
            rpcReject(req.signal.reason);
        });

        this.outstanding.set(id, { resolve: rpcResolve, reject: rpcReject });
        try {
            await this.channel.write(
                new Rpc({
                    id: BigInt(id),
                    header: {
                        method: methodName(req),
                        headers: headersToRpcHeaders(req.header),
                    },
                    body: {
                        data: serdes.getI(true).serialize(req.message),
                    },
                    trailer: {},
                }),
            );
            ret = await rpcPromise;
        }
        finally {
            this.outstanding.delete(id);
        }

        if (ret.status && ret.status?.code != 0) {
            throw new ConnectError(
                ret.status?.message || "Unknown",
                ret.status?.code,
                undefined,
                ret.status?.details,
            );
        }
        else if (ret.body) {
            const msg = serdes.getO(true).parse(ret.body.data);
            return {
                stream: false,
                service: req.service,
                method: req.method,
                header: rpcHeadersToHeaders(ret.header?.headers),
                trailer: rpcHeadersToHeaders(ret.trailer?.metadata),
                message: msg,
            };
        }
        else {
            // No body defined, no status... Invalid?
            // FIXME: error handling here
            throw new ConnectError("invalid response");
        }
    }

    private async performStreaming<I extends Message<I> = AnyMessage, O extends Message<O> = AnyMessage>(req: StreamRequest<I, O>): Promise<StreamResponse<I, O>> {
        const serdes = createMethodSerializationLookup(req.method, undefined, undefined, { writeMaxBytes: 10000000, readMaxBytes: 10000000 });
        const id = this.nextId++;
        const outputIterable = createWritableIterable<Rpc | Error>();
        const requestHeader = new RequestHeader({
            method: methodName(req),
            headers: headersToRpcHeaders(req.header),
        });
        const notifyAbort = () => {
            // Note when we write to outputIterable in a callback like this outside of an async
            // context we can't be sure of ordering -- it might be we've hit an error before this
            // and the consumer has stopped reading from our output. So be sure to catch() and
            // ignore any errors in such a case; else we're left with dangling promises that at
            // the very least cause test failures due to an "Unhandled Rejection".
            outputIterable.write(new Error(req.signal.reason)).catch(() => {});
        };
        const cleanup = () => {
            this.outstanding.delete(id);
            outputIterable.close();
            req.signal.removeEventListener("abort", notifyAbort);
        };

        req.signal.throwIfAborted();
        req.signal.addEventListener("abort", notifyAbort);

        // Configure how we deal with responses first
        this.outstanding.set(id, {
            resolve: rpc => {
                outputIterable.write(rpc).catch(() => {});
            },
            reject: reason => {
                outputIterable.write(reason).catch(() => {});
            },
        });

        try {
            // Streaming RPCs are always started with an explicit message which contains no body.
            await this.channel.write(
                new Rpc({
                    id: BigInt(id),
                    header: requestHeader,
                }),
            );

            // Start an async operation to stream our messages. In the case of a ServerStream,
            // there will just be a single message to upload. In other cases there can be many.
            const uploadPromise = new Promise<void>(async (resolve, reject) => {
                try {
                    for await (const upload of req.message) {
                        await this.channel.write(
                            new Rpc({
                                id: BigInt(id),
                                header: requestHeader,
                                body: {
                                    data: serdes.getI(true).serialize(upload),
                                },
                            }),
                        );
                    }

                    // Need to send an "end stream" command now, which means specifying a `trailer`
                    await this.channel.write(
                        new Rpc({
                            id: BigInt(id),
                            header: requestHeader,
                            trailer: {},
                        }),
                    );
                }
                catch (err) {
                    reject(err);
                    return;
                }
                resolve();
            });
            uploadPromise.catch(err => {
                outputIterable.write(new Error(`upload error: ${err}`));
            });

            return {
                stream: true,
                service: req.service,
                method: req.method,
                header: req.header,
                trailer: new Headers(),
                message: pipe(
                    outputIterable,
                    async function* (iterable) {
                        try {
                            for await (const rpc of iterable) {
                                if (rpc instanceof Error) {
                                    throw rpc;
                                }
                                if (rpc.status && rpc.status.code != 0) {
                                    throw new ConnectError(rpc.status.message, rpc.status.code, undefined, rpc.status.details);
                                }
                                if (rpc.body) {
                                    yield serdes.getO(true).parse(rpc.body.data);
                                }
                                if (rpc.trailer) {
                                    return;
                                }
                            }
                        }
                        finally {
                            cleanup();
                        }
                    },
                    { propagateDownStreamError: true },
                ),
            };
        }
        catch (err) {
            cleanup();
            throw err;
        }
    }
}

function rpcHeadersToHeaders(reqHeader: KeyValue[] | undefined): Headers {
    if (!reqHeader) {
        return new Headers([]);
    }
    return new Headers(reqHeader.map(kv => {
        return [kv.key, kv.value];
    }));
}

function headersToRpcHeaders(input: HeadersInit | undefined): KeyValue[] {
    const headers: KeyValue[] = [];
    new Headers(input).forEach((value, key) => {
        headers.push(new KeyValue({ key: key, value: value }));
    });
    return headers;
}

function methodName<I extends Message<I> = AnyMessage, O extends Message<O> = AnyMessage>(req: StreamRequest<I, O> | UnaryRequest<I, O>): string {
    return `/${req.service.typeName}/${req.method.name}`;
}

// ~polyfill for Promise.withResolvers(), which was only introduced recently and is not supported everywhere.
// See: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Promise/withResolvers
function promiseWithResolvers<T>() {
    var resolver: ((value: T) => void) | undefined = undefined;
    var rejecter: ((value: any) => void) | undefined = undefined;
    const promise = new Promise<T>((resolve, reject) => {
        resolver = resolve;
        rejecter = reject;
    });
    return {
        promise: promise,
        resolve: resolver!,
        reject: rejecter!,
    };
}
