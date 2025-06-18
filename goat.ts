import { type KeyValue, KeyValueSchema, RequestHeaderSchema, type Rpc, RpcSchema } from "./gen/goatorepo/rpc_pb";
import { Code, type ContextValues, createContextValues, type Transport, type StreamResponse, type UnaryRequest, type UnaryResponse, ConnectError, type StreamRequest, type Interceptor } from "@connectrpc/connect";
import { runUnaryCall, runStreamingCall, createMethodSerializationLookup, createWritableIterable, pipe } from "@connectrpc/connect/protocol";
import { AwaitableQueue } from "./util";
import { create, type DescMessage, type DescMethodStreaming, type DescMethodUnary, type MessageInitShape } from "@bufbuild/protobuf";

export { type Rpc, AwaitableQueue };

export interface RpcReadWriter {
    read(): Promise<Rpc>;
    write(rpc: Rpc): Promise<void>;
    done(): void;
}

export interface GoatConfig {
    destinationName?: string;
    sourceName?: string;
    interceptors?: Interceptor[];
}

export class GoatTransport implements Transport {
    channel: RpcReadWriter;
    destination?: string;
    source?: string;
    outstanding = new Map<number, { resolve: (rpc: Rpc) => void; reject: (reason: any) => void; }>();
    // GOAT uses 64-bit ints (bigint) for IDs natively, but it's fine to use
    // "number" as we're in charge of allocating them here, and there are enough
    // integers available to not overflow (Number.MAX_SAFE_INTEGER ~= 2^53)
    nextId: number = 0;
    readError: any = undefined;
    interceptors: Interceptor[] = [];

    constructor(ch: RpcReadWriter, cfg?: GoatConfig) {
        this.channel = ch;
        this.destination = cfg?.destinationName;
        this.source = cfg?.sourceName;
        this.interceptors = cfg?.interceptors || [];

        this.startReader();
    }

    reset(newChannel: RpcReadWriter, reason?: any) {
        for (const value of this.outstanding.values()) {
            value.reject(reason ?? new ConnectError("reset", Code.Aborted));
        }
        this.outstanding.clear();

        const oldChannel = this.channel;
        this.channel = newChannel;
        this.readError = undefined;

        this.startReader();
        oldChannel.done();
    }

    private startReader(): void {
        const initialChannel = this.channel;

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
                if (this.channel != initialChannel) {
                    // We've had .reset() called before this -- ignore the error.
                    return;
                }

                this.readError = reason;

                for (const value of this.outstanding.values()) {
                    value.reject(reason);
                }
                this.outstanding.clear();
            });
    }

    unary<I extends DescMessage, O extends DescMessage>(
        method: DescMethodUnary<I, O>,
        signal: AbortSignal | undefined,
        timeoutMs: number | undefined,
        header: HeadersInit | undefined,
        input: MessageInitShape<I>,
        contextValues?: ContextValues,
    ): Promise<UnaryResponse<I, O>> {
        if (this.readError) {
            throw new Error(this.readError);
        }

        return runUnaryCall({
            interceptors: this.interceptors,
            signal: signal,
            timeoutMs: timeoutMs,
            req: {
                stream: false,
                method: method,
                service: method.parent,
                header: new Headers(header),
                contextValues: contextValues ?? createContextValues(),
                url: "",
                message: input,
                requestMethod: "POST",
            },
            next: req => {
                return this.performUnary(req);
            },
        });
    }

    stream<I extends DescMessage, O extends DescMessage>(
        method: DescMethodStreaming<I, O>,
        signal: AbortSignal | undefined,
        timeoutMs: number | undefined,
        header: HeadersInit | undefined,
        input: AsyncIterable<MessageInitShape<I>>,
        contextValues?: ContextValues,
    ): Promise<StreamResponse<I, O>> {
        if (this.readError) {
            throw (new Error(this.readError));
        }

        return runStreamingCall({
            interceptors: this.interceptors,
            signal: signal,
            timeoutMs: timeoutMs,
            req: {
                stream: true,
                service: method.parent,
                method: method,
                header: new Headers(header),
                contextValues: contextValues ?? createContextValues(),
                url: "",
                message: input,
                requestMethod: "POST",
            },
            next: req => {
                return this.performStreaming(req);
            },
        });
    }

    private async performUnary<I extends DescMessage, O extends DescMessage>(req: UnaryRequest<I, O>): Promise<UnaryResponse<I, O>> {
        const serdes = createMethodSerializationLookup(req.method, undefined, undefined, { writeMaxBytes: 10000000, readMaxBytes: 10000000 });
        const id = this.nextId++;
        const { promise: rpcPromise, resolve: rpcResolve, reject: rpcReject } = promiseWithResolvers<Rpc>();
        let ret: Rpc;

        req.signal.throwIfAborted();
        req.signal.addEventListener("abort", () => {
            rpcReject(req.signal.reason);
        });

        this.outstanding.set(id, { resolve: rpcResolve, reject: rpcReject });
        try {
            await this.channel.write(
                create(RpcSchema, {
                    id: BigInt(id),
                    header: {
                        method: methodName(req),
                        headers: headersToRpcHeaders(req.header),
                        destination: this.destination,
                        source: this.source,
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

    private async performStreaming<I extends DescMessage, O extends DescMessage>(req: StreamRequest<I, O>): Promise<StreamResponse<I, O>> {
        const serdes = createMethodSerializationLookup(req.method, undefined, undefined, { writeMaxBytes: 10000000, readMaxBytes: 10000000 });
        const id = this.nextId++;
        const outputIterable = createWritableIterable<Rpc | Error>();
        const initialRequestHeader = create(RequestHeaderSchema, {
            method: methodName(req),
            headers: headersToRpcHeaders(req.header),
            destination: this.destination,
            source: this.source,
        });
        const requestHeader = create(RequestHeaderSchema, {
            method: methodName(req),
            // Doesn't include `headers` -- this is only sent in the initial message
            destination: this.destination,
            source: this.source,
        });
        const notifyAbort = () => {
            // Note when we write to outputIterable in a callback like this outside of an async
            // context we can't be sure of ordering -- it might be we've hit an error before this
            // and the consumer has stopped reading from our output. So be sure to catch() and
            // ignore any errors in such a case; else we're left with dangling promises that at
            // the very least cause test failures due to an "Unhandled Rejection".
            if (req.signal.reason instanceof Error || req.signal.reason instanceof DOMException) {
                outputIterable.write(req.signal.reason).catch(() => {});
            }
            else {
                outputIterable.write(new DOMException(req.signal.reason, "AbortError")).catch(() => {});
            }
        };
        let serverHasClosedStream = false;
        let clientHasClosedStream = false;
        const cleanup = () => {
            this.outstanding.delete(id);
            outputIterable.close();

            // If, when we are cleaning up here, we have yet to fully finish the stream, ensure
            // we send an abort message if at all possible (i.e. if the channel is working).
            //
            // GRPC docs: "When an application or runtime error occurs during an RPC a Status and Status-Message
            // are delivered in Trailers.
            // In some cases it is possible that the framing of the message stream has become corrupt and the RPC
            // runtime will choose to use an RST_STREAM frame to indicate this state to its peer. RPC runtime
            // implementations should interpret RST_STREAM as immediate full-closure of the stream and should
            // propagate an error up to the calling application layer."
            if (!serverHasClosedStream || !clientHasClosedStream) {
                this.channel.write(
                    create(RpcSchema, {
                        id: BigInt(id),
                        header: requestHeader,
                        reset: {
                            type: "RST_STREAM",
                        },
                    }),
                ).catch(() => {});
            }

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
                create(RpcSchema, {
                    id: BigInt(id),
                    header: initialRequestHeader,
                }),
            );

            // Start an async operation to stream our messages. In the case of a ServerStream,
            // there will just be a single message to upload. In other cases there can be many.
            const uploadPromise = (async () => {
                for await (const upload of req.message) {
                    await this.channel.write(
                        create(RpcSchema, {
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
                    create(RpcSchema, {
                        id: BigInt(id),
                        header: requestHeader,
                        trailer: {},
                    }),
                );
                clientHasClosedStream = true;
            })();
            uploadPromise.catch(err => {
                outputIterable.write(new Error(`upload error: ${err}`)).catch(() => {});
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
                                    serverHasClosedStream = true;
                                    throw new ConnectError(rpc.status.message, rpc.status.code, undefined, undefined, rpc.status.details);
                                }
                                if (rpc.body) {
                                    yield serdes.getO(true).parse(rpc.body.data);
                                }
                                if (rpc.trailer) {
                                    serverHasClosedStream = true;
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
        return [kv.key, kv.value] satisfies [string, string];
    }));
}

function headersToRpcHeaders(input: HeadersInit | undefined): KeyValue[] {
    const headers: KeyValue[] = [];
    new Headers(input).forEach((value, key) => {
        headers.push(create(KeyValueSchema, { key: key, value: value }));
    });
    return headers;
}

function methodName<I extends DescMessage, O extends DescMessage>(req: StreamRequest<I, O> | UnaryRequest<I, O>): string {
    return `/${req.service.typeName}/${req.method.name}`;
}

// ~polyfill for Promise.withResolvers(), which was only introduced recently and is not supported everywhere.
// See: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Promise/withResolvers
function promiseWithResolvers<T>() {
    let resolver: ((value: T) => void) | undefined = undefined;
    let rejecter: ((value: any) => void) | undefined = undefined;
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
