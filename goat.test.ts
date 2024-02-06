import { Code, createPromiseClient } from "@connectrpc/connect";
import { createAsyncIterable } from "@connectrpc/connect/protocol";
import { TestService } from "gen/testproto/test_connect";
import { Msg } from "gen/testproto/test_pb";
import { Body, RequestHeader, ResponseStatus, Rpc } from "gen/goatorepo/rpc_pb";
import { GoatTransport } from "goat";
import { vi } from "vitest";
import { AwaitableQueue } from "./util";

const newFifoMockReadWrite = function () {
    const fifo = new AwaitableQueue<Rpc>();
    const mockRpcReadWrite = {
        read: vi.fn<[], Promise<Rpc>>(),
        write: vi.fn<[Rpc], Promise<void>>(),
        done: vi.fn<[], void>(),
    };

    vi.mocked(mockRpcReadWrite.write).mockImplementation(rpc => {
        fifo.push(rpc);
        return Promise.resolve();
    });
    vi.mocked(mockRpcReadWrite.read).mockImplementation(async () => {
        const rpc = await fifo.pop();
        return Promise.resolve(rpc);
    });
    return mockRpcReadWrite;
};

describe("unit: unary RPC", () => {
    it("performs simple requests/responses", async () => {
        const transport = new GoatTransport(newFifoMockReadWrite());
        const ts = createPromiseClient(TestService, transport);

        for (var i = 0; i < 10; i++) {
            const ret = await ts.unary(new Msg({ value: i }));
            expect(ret.value).toBe(i);
        }
    });

    it("performs back to back", async () => {
        const transport = new GoatTransport(newFifoMockReadWrite());
        const ts = createPromiseClient(TestService, transport);
        const reqs: Promise<Msg>[] = [];

        for (var i = 0; i < 10; i++) {
            reqs.push(ts.unary(new Msg({ value: i })));
        }

        const results = await Promise.all(reqs);

        for (var i = 0; i < 10; i++) {
            expect(results[i].value).toBe(i);
        }
    });

    it("errors on response status", async () => {
        var first = true;
        const readRpcErr = {
            read: () => {
                if (first) {
                    first = false;
                    return Promise.resolve(
                        new Rpc({
                            id: BigInt(0),
                            status: new ResponseStatus({
                                code: Code.InvalidArgument,
                                message: "Yo, you passed an invalid argument dawg",
                                // TODO: look into what might got into details
                                details: [],
                            }),
                        }),
                    );
                }
                return new Promise<Rpc>((_, reject) => {
                    setTimeout(reject, 10000);
                });
            },
            write: () => {
                return Promise.resolve();
            },
            done: () => {},
        };

        const transport = new GoatTransport(readRpcErr);
        const ts = createPromiseClient(TestService, transport);

        await expect(async () => {
            await ts.unary({});
        }).rejects.toThrow("Yo, you passed an invalid argument dawg");
    });

    it("handles abort before RPC", async () => {
        const transport = new GoatTransport(newFifoMockReadWrite());
        const ts = createPromiseClient(TestService, transport);
        const signalController: AbortController = new AbortController();

        await expect(async () => {
            signalController.abort();
            await ts.unary(new Msg({ value: 1 }), { signal: signalController.signal });
        }).rejects.toThrow("This operation was aborted");
    });

    it("handles abort during RPC", async () => {
        const mockRrw = newFifoMockReadWrite();

        mockRrw.read.mockImplementation(() => {
            // Don't resolve, just block indefinitely
            return new Promise<Rpc>(() => {});
        });

        const transport = new GoatTransport(mockRrw);
        const ts = createPromiseClient(TestService, transport);
        const signalController: AbortController = new AbortController();

        await expect(async () => {
            const rpc = ts.unary(new Msg({ value: 1 }), { signal: signalController.signal });
            signalController.abort();
            await rpc;
        }).rejects.toThrow("[canceled] This operation was aborted");
    });

    it("handles read error during RPC", async () => {
        const mockRrw = newFifoMockReadWrite();
        var readRejected: ((reason: any) => void) | undefined = undefined;

        mockRrw.read.mockImplementation(() => {
            // Don't resolve, just block indefinitely
            return new Promise<Rpc>((_, reject) => {
                readRejected = reject;
            });
        });

        const transport = new GoatTransport(mockRrw);
        const ts = createPromiseClient(TestService, transport);

        await expect(async () => {
            const rpc = ts.unary(new Msg({ value: 1 }));
            readRejected!(new Error("Read error"));
            await rpc;
        }).rejects.toThrow("Read error");

        // Now that we're in the read error state, any RPC attempts should immediately fail
        await expect(async () => {
            await ts.unary(new Msg({ value: 1 }));
        }).rejects.toThrow("Read error");
    });

    it("handles reset after read error", async () => {
        const mockRrw = newFifoMockReadWrite();
        var readRejected: ((reason: any) => void) | undefined = undefined;

        mockRrw.read.mockImplementation(() => {
            // Don't resolve, just block indefinitely
            return new Promise<Rpc>((_, reject) => {
                readRejected = reject;
            });
        });

        const transport = new GoatTransport(mockRrw);
        const ts = createPromiseClient(TestService, transport);

        await expect(async () => {
            const rpc = ts.unary(new Msg({ value: 1 }));
            readRejected!(new Error("Read error"));
            await rpc;
        }).rejects.toThrow("Read error");

        // Now that we're in the read error state, any RPC attempts should immediately fail
        await expect(async () => {
            await ts.unary(new Msg({ value: 1 }));
        }).rejects.toThrow("Read error");

        transport.reset(newFifoMockReadWrite());

        const ret = await ts.unary(new Msg({ value: 51 }));
        expect(ret.value).toBe(51);
    });

    it("handles reset of ongoing RPCs", async () => {
        const mockRrw = newFifoMockReadWrite();
        let readHasStartedResolve: (value: void | PromiseLike<void>) => void;
        const readHasStarted = new Promise<void>(res => {
            readHasStartedResolve = res;
        });

        mockRrw.read.mockImplementation(() => {
            readHasStartedResolve();
            // Don't resolve, just block indefinitely
            return new Promise<Rpc>(() => {});
        });

        const transport = new GoatTransport(mockRrw);
        const ts = createPromiseClient(TestService, transport);

        const initialRpc = expect(async () => {
            await ts.unary(new Msg({ value: 1 }));
        }).rejects.toThrow("[aborted] reset");

        await readHasStarted;

        transport.reset(newFifoMockReadWrite());

        await initialRpc;

        const ret = await ts.unary(new Msg({ value: 51 }));
        expect(ret.value).toBe(51);
    });

    it("sends headers", async () => {
        const mockRrw = newFifoMockReadWrite();
        var reqHeader: RequestHeader | undefined;

        const origRead = mockRrw.read.getMockImplementation();
        mockRrw.read.mockImplementation(async () => {
            const rpc = await origRead!!();
            reqHeader = rpc.header;
            return Promise.resolve(rpc);
        });

        const transport = new GoatTransport(mockRrw);
        const ts = createPromiseClient(TestService, transport);

        const res = await ts.unary(
            { value: 12 },
            {
                headers: new Headers({
                    "x-foo": "bar",
                    "sam": "was here",
                }),
            },
        );
        expect(res.value).toBe(12);

        expect(reqHeader).toBeTruthy();
        expect(reqHeader?.headers.length).toBe(2);
        expect(reqHeader?.headers[1].key).toBe("x-foo");
        expect(reqHeader?.headers[1].value).toBe("bar");
    });
});

describe("unit: streaming RPCs", () => {
    enum State {
        NotStarted = 1,
        Started = 2,
        Ended = 3,
    }

    beforeEach(() => {
        vi.useFakeTimers();
    });
    afterEach(() => {
        vi.runAllTimers();
        vi.useRealTimers();
    });

    class MockClientStreamResponder {
        private state: State = State.NotStarted;
        private q = new AwaitableQueue<Rpc>();
        private onMsg: (rpc: Rpc) => void = (_: Rpc) => {};
        private onEnd: () => [Body?, ResponseStatus?] = () => {
            return [undefined, undefined];
        };

        mockOnMsg(fn: (rpc: Rpc) => Promise<void>) {
            this.onMsg = vi.fn();
            vi.mocked(this.onMsg).mockImplementation(fn);
        }
        mockOnEnd(fn: () => [Body?, ResponseStatus?]) {
            this.onEnd = vi.fn();
            vi.mocked(this.onEnd).mockImplementation(fn);
        }

        async read() {
            return this.q.pop();
        }
        async write(rpc: Rpc) {
            switch (this.state) {
                case State.NotStarted:
                    if (rpc.body) {
                        await this.onMsg(rpc);
                    }

                    this.q.push(
                        new Rpc({
                            id: rpc.id,
                            header: rpc.header,
                        }),
                    );
                    this.state = State.Started;
                    break;
                case State.Started:
                    if (rpc.body) {
                        await this.onMsg(rpc);
                    }
                    if (rpc.trailer) {
                        // End of client stream, send a response
                        const [body, status] = this.onEnd();
                        this.q.push(
                            new Rpc({
                                id: rpc.id,
                                header: rpc.header,
                                body: body,
                                status: status,
                                trailer: {},
                            }),
                        );
                        this.state = State.Ended;
                    }
                    break;
            }
        }

        done() {}
    }

    class MockServerStreamResponder {
        private q = new AwaitableQueue<Rpc>();

        async read() {
            return this.q.pop();
        }
        async write(rpc: Rpc) {
            if (!rpc.body?.data && !rpc.trailer) {
                // Acknowledge the stream
                this.q.push(
                    new Rpc({
                        id: rpc.id,
                        header: rpc.header,
                    }),
                );
                return;
            }
            if (!rpc.body?.data) {
                return;
            }

            const input = new Msg({}).fromBinary(rpc.body.data);

            for (var i = 0; i < input.value; i++) {
                this.q.push(
                    new Rpc({
                        id: rpc.id,
                        header: rpc.header,
                        body: {
                            data: new Msg({ value: 1 }).toBinary(),
                        },
                    }),
                );
            }

            // End-of-stream message
            this.q.push(
                new Rpc({
                    id: rpc.id,
                    header: rpc.header,
                    trailer: {},
                }),
            );
        }

        done() {}
    }

    class MockBidirStreamResponder {
        private q = new AwaitableQueue<Rpc>();
        public record: Rpc[] = [];

        async read() {
            return this.q.pop();
        }
        async write(rpc: Rpc) {
            this.record.push(rpc);

            if (!rpc.body?.data && !rpc.trailer) {
                // Acknowledge the stream
                this.q.push(
                    new Rpc({
                        id: rpc.id,
                        header: rpc.header,
                    }),
                );
                return;
            }

            if (rpc.trailer) {
                this.q.push(
                    new Rpc({
                        id: rpc.id,
                        header: rpc.header,
                        trailer: {},
                    }),
                );
                return;
            }

            if (!rpc.body?.data) {
                return;
            }

            this.q.push(
                new Rpc({
                    id: rpc.id,
                    header: rpc.header,
                    body: rpc.body,
                }),
            );
        }

        done() {}
    }

    it("handles client stream write error", async () => {
        const mock = new MockClientStreamResponder();
        mock.mockOnEnd(() => {
            throw new Error("write error");
        });
        const transport = new GoatTransport(mock);
        const ts = createPromiseClient(TestService, transport);

        await expect(async () => {
            await ts.clientStream(createAsyncIterable([
                new Msg({ value: 1 }),
                new Msg({ value: 3 }),
            ]));
        }).rejects.toThrow("upload error");
    });

    it("handles client stream abort", async () => {
        const mock = new MockClientStreamResponder();
        mock.mockOnEnd(() => {
            signalController.abort();
            return [undefined, undefined];
        });
        const transport = new GoatTransport(mock);
        const ts = createPromiseClient(TestService, transport);
        const signalController: AbortController = new AbortController();

        await expect(async () => {
            await ts.clientStream(
                createAsyncIterable([
                    new Msg({ value: 1 }),
                    new Msg({ value: 3 }),
                ]),
                { signal: signalController.signal },
            );
        }).rejects.toThrow("This operation was aborted");
    });

    it("handles client stream timeout", async () => {
        const mock = new MockClientStreamResponder();
        mock.mockOnMsg(() => {
            return new Promise<void>(resolve => {
                setTimeout(resolve, 1000);
            });
        });
        const transport = new GoatTransport(mock);
        const ts = createPromiseClient(TestService, transport);

        const p = expect(async () => {
            await ts.clientStream(
                createAsyncIterable([
                    new Msg({ value: 1 }),
                    new Msg({ value: 3 }),
                ]),
                { timeoutMs: 2 },
            );
        }).rejects.toThrow("the operation timed out");

        for (var i = 0; i < 10; i++) {
            await vi.advanceTimersByTimeAsync(250);
        }

        await p;
    });

    it("performs client stream", async () => {
        var count = 0;
        const mock = new MockClientStreamResponder();
        mock.mockOnMsg((rpc: Rpc) => {
            if (rpc.body) {
                const msg: Msg = new Msg({});
                msg.fromBinary(rpc.body.data);
                count += msg.value;
            }
            return Promise.resolve();
        });
        mock.mockOnEnd(() => {
            const msg = new Msg({ value: count });
            return [new Body({ data: msg.toBinary() }), undefined];
        });
        const transport = new GoatTransport(mock);
        const ts = createPromiseClient(TestService, transport);

        const ret = await ts.clientStream(createAsyncIterable([
            new Msg({ value: 1 }),
            new Msg({ value: 3 }),
        ]));
        expect(ret.value).toBe(4);
    });

    it("performs server stream", async () => {
        const mock = new MockServerStreamResponder();

        const transport = new GoatTransport(mock);
        const ts = createPromiseClient(TestService, transport);
        var count = 0;

        for await (const resp of ts.serverStream(new Msg({ value: 3 }))) {
            expect(resp.value).toBe(1);
            count++;
        }

        expect(count).toBe(3);
    });

    it("performs bidir stream", async () => {
        const mock = new MockBidirStreamResponder();

        const transport = new GoatTransport(mock);
        const ts = createPromiseClient(TestService, transport);

        const ret = await ts.bidiStream(createAsyncIterable([
            new Msg({ value: 1 }),
            new Msg({ value: 3 }),
        ]));
        var count = 0;
        for await (const msg of ret) {
            count += msg.value;
        }
        expect(count).toBe(4);
    });

    it("closes stream on abort signal", async () => {
        const mock = new MockBidirStreamResponder();

        const transport = new GoatTransport(mock);
        const ts = createPromiseClient(TestService, transport);
        const signalController: AbortController = new AbortController();

        var finish: ((value: void | PromiseLike<void>) => void) | undefined;
        const finishPromise = new Promise<void>(res => {
            finish = res;
        });
        expect(finish).toBeDefined();

        // Start the streaming RPC then block...
        const ret = await ts.bidiStream(
            (async function* () {
                yield new Msg({ value: 77 });
                await finishPromise;

                // XX: what happens if an exception is thrown in here?
            })(),
            {
                signal: signalController.signal,
            },
        );

        for await (const msg of ret) {
            expect(msg.value).toBe(77);
            break;
        }

        signalController.abort("test abort");

        expect(async () => {
            var count = 0;
            for await (const _ of ret) {
                count++;
            }
            expect(count).toBe(0);
        }).rejects.toThrow("test abort");

        // At this point our upload loop is stuck on "await finishPromise".
        // So finish that loop off, and run any outstanding work.
        if (finish) finish();
        await vi.runAllTimersAsync();

        // The last RPC should have "trailer" set.
        expect(mock.record.length).toBeGreaterThanOrEqual(1);
        const lastRpc = mock.record[mock.record.length - 1];

        expect(lastRpc.body).not.toBeDefined();
        expect(lastRpc.trailer).toBeDefined();
    });

    it("closes server stream on abort signal", async () => {
        class MockInfiniteServerStreamResponder {
            private q = new AwaitableQueue<Rpc>();
            public record: Rpc[] = [];

            async read() {
                return this.q.pop();
            }
            async write(rpc: Rpc) {
                this.record.push(rpc);

                if (!rpc.body?.data && !rpc.trailer) {
                    // Acknowledge the stream
                    this.q.push(
                        new Rpc({
                            id: rpc.id,
                            header: rpc.header,
                        }),
                    );
                    return;
                }

                // Never send end-of-stream
            }

            done() {}
        }

        const mock = new MockInfiniteServerStreamResponder();
        const transport = new GoatTransport(mock);
        const ts = createPromiseClient(TestService, transport);
        const signalController: AbortController = new AbortController();

        const rpcPromise = expect(async () => {
            for await (
                const _resp of ts.serverStream(new Msg({ value: 3 }), {
                    signal: signalController.signal,
                })
            ) {
                // We'll be stuck now...
            }
        }).rejects.toThrow("This operation was aborted");

        await vi.advanceTimersByTimeAsync(1000);

        // Messages so far:
        // - start stream
        // - single message with body
        // - end stream
        expect(mock.record.length).toBe(3);
        expect(mock.record[2].trailer).toBeDefined();
        expect(mock.record[2].status).not.toBeDefined();

        signalController.abort();

        await rpcPromise;

        // Now the abort should have resulted in an abort stream message
        expect(mock.record.length).toBe(4);
        expect(mock.record[3].trailer).toBeDefined();
        expect(mock.record[3].status).toBeDefined();
        expect(mock.record[3].status?.code).toBe(Code.Aborted);
    });

    it("handles exception in async iterable for upload", async () => {
    });
});
