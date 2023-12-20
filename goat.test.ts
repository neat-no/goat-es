import { Code, createPromiseClient } from "@connectrpc/connect";
import { createAsyncIterable } from "@connectrpc/connect/protocol";
import { TestService } from "gen/testproto/test_connect";
import { Msg } from "gen/testproto/test_pb";
import { Body, RequestHeader, ResponseStatus, Rpc } from "gen/goatorepo/rpc_pb";
import { GoatTransport } from "goat";
import { vi } from "vitest";

class AwaitableQueue<T> {
    private q = Array<T>();
    private notify?: () => void;

    push(item: T) {
        this.q.push(item);
        if (this.notify) {
            this.notify();
        }
    }
    async pop(): Promise<T> {
        while (this.q.length == 0) {
            await new Promise<void>(r => {
                this.notify = r;
            });
        }
        return this.q.shift()!!; 
    }
}

const newFifoMockReadWrite = function () {
    const fifo = new AwaitableQueue<Rpc>();
    const mockRpcReadWrite = {
        read: vi.fn<[], Promise<Rpc>>(),
        write: vi.fn<[Rpc], Promise<void>>(),
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

describe("unary RPC", () => {
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
        };

        const transport = new GoatTransport(readRpcErr);
        const ts = createPromiseClient(TestService, transport);

        expect(async () => {
            await ts.unary({});
        }).rejects.toThrow("Yo, you passed an invalid argument dawg");
    });

    it("handles abort before RPC", async () => {
        const transport = new GoatTransport(newFifoMockReadWrite());
        const ts = createPromiseClient(TestService, transport);
        const signalController: AbortController = new AbortController();

        expect(async () => {
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

        expect(async () => {
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

        expect(async () => {
            const rpc = ts.unary(new Msg({ value: 1 }));
            readRejected!(new Error("Read error"));
            await rpc;
        }).rejects.toThrow("Read error");

        // Now that we're in the read error state, any RPC attempts should immediately fail
        expect(async () => {
            await ts.unary(new Msg({ value: 1 }));
        }).rejects.toThrow("Read error");
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

describe("Streaming RPCs", () => {
    enum State {
        NotStarted = 1,
        Started = 2,
        Ended = 3,
    }

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
    }

    class MockBidirStreamResponder {
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
    }

    it("handles client stream write error", async () => {
        const mock = new MockClientStreamResponder();
        mock.mockOnEnd(() => {
            throw new Error("write error");
        });
        const transport = new GoatTransport(mock);
        const ts = createPromiseClient(TestService, transport);

        expect(async () => {
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

        expect(async () => {
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

        expect(async () => {
            await ts.clientStream(
                createAsyncIterable([
                    new Msg({ value: 1 }),
                    new Msg({ value: 3 }),
                ]),
                { timeoutMs: 2 },
            );
        }).rejects.toThrow("the operation timed out");
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
});
