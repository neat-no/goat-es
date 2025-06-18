import { type Rpc, RpcSchema } from "gen/goatorepo/rpc_pb";
import { GoatTransport, type RpcReadWriter } from "goat";
import { AwaitableQueue } from "./util";
import WebSocket from "ws";
import { MsgSchema, TestService } from "gen/testproto/test_pb";
import { createClient } from "@connectrpc/connect";
import { createAsyncIterable } from "@connectrpc/connect/protocol";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";

const e2e_test_addr = process.env.E2E_TEST_ADDR ?? "ws://localhost:9043/test";

if (!(e2e_test_addr.startsWith("ws://") || e2e_test_addr.startsWith("wss://"))) {
    console.error(`Destination must start with ws[s]:// (got: ${e2e_test_addr})`);
    process.exit(1);
}

class WebsocketRpcs implements RpcReadWriter {
    private ws: WebSocket;
    private queue = new AwaitableQueue<Rpc>();

    constructor(dest: string) {
        this.ws = new WebSocket(dest);

        this.ws.on("error", console.error);

        this.ws.on("message", data => {
            try {
                const rpc = fromBinary(RpcSchema, data as Uint8Array);
                this.queue.push(rpc);
            }
            catch (err) {
                console.error(err);
            }
        });
    }

    async connect(): Promise<void> {
        await new Promise<void>((res, rej) => {
            this.ws.on("open", res);
            this.ws.on("error", (_ws: WebSocket, err: Error) => {
                rej(err);
            });
        });
    }

    async disconnect(): Promise<void> {
        await new Promise<void>(res => {
            this.ws.on("close", res);
            this.ws.close(1000, "ok");
        });
    }

    async read(): Promise<Rpc> {
        return this.queue.pop();
    }

    async write(rpc: Rpc): Promise<void> {
        this.ws.send(toBinary(RpcSchema, rpc));
    }

    done() {}
}

async function fromAsync<T>(iter: AsyncIterable<T>): Promise<T[]> {
    const arr: T[] = [];
    for await (const i of iter) {
        arr.push(i);
    }
    return arr;
}

describe("integration: e2e", () => {
    it("connects websocket", async () => {
        const rpcs = new WebsocketRpcs(e2e_test_addr);
        await rpcs.connect();
        await rpcs.disconnect();
    });

    it("performs unary", async () => {
        const rpcs = new WebsocketRpcs(e2e_test_addr);
        await rpcs.connect();

        const t = new GoatTransport(rpcs, { destinationName: "e2e", sourceName: "source" });
        const ts = createClient(TestService, t);

        const val = await ts.unary(create(MsgSchema, { value: 21 }));
        expect(val.value).toBe(42);

        await rpcs.disconnect();
    });

    it("unary has trailers", async () => {
        const rpcs = new WebsocketRpcs(e2e_test_addr);
        await rpcs.connect();

        const transport = new GoatTransport(rpcs, { destinationName: "e2e" });
        const ts = createClient(TestService, transport);
        let trailerCount = 0, headerCount = 0;

        const ret = await ts.unary(create(MsgSchema, { value: 123 }), {
            onHeader: headers => {
                expect(headers.get("foo")).toBe("baz");
                headerCount++;
            },
            onTrailer: trailers => {
                expect(trailers.get("timestamp")).toContain(`${new Date().getFullYear()}-`);
                expect(trailers.get("foo")).toBe("bar");
                expect(trailers.get("input")).toBe("123");
                trailerCount++;
            },
        });
        expect(ret.value).toBe(246);
        expect(headerCount).toBe(1);
        expect(trailerCount).toBe(1);

        await rpcs.disconnect();
    });

    it("performs server stream", async () => {
        const rpcs = new WebsocketRpcs(e2e_test_addr);
        await rpcs.connect();

        const t = new GoatTransport(rpcs, { destinationName: "e2e" });
        const ts = createClient(TestService, t);

        const arr = await fromAsync(ts.serverStream(create(MsgSchema, { value: 6 })));

        expect(arr.map(m => m.value)).toStrictEqual([0, 1, 2, 3, 4, 5]);

        await rpcs.disconnect();
    });

    it("aborts server stream", async () => {
        const rpcs = new WebsocketRpcs(e2e_test_addr);
        await rpcs.connect();

        const t = new GoatTransport(rpcs, { destinationName: "e2e" });
        const ts = createClient(TestService, t);
        const ab = new AbortController();

        const streamPromise = expect(async () => {
            for await (const ret of ts.serverStreamThatSleeps(create(MsgSchema, { value: 60 }), { signal: ab.signal })) {
                // We should never get here!
                expect(ret).toBeUndefined();
            }
        }).rejects.toThrow("This operation was aborted");

        ab.abort();

        await streamPromise;

        await rpcs.disconnect();
    });

    it("performs client stream", async () => {
        const rpcs = new WebsocketRpcs(e2e_test_addr);
        await rpcs.connect();

        const t = new GoatTransport(rpcs, { destinationName: "e2e" });
        const ts = createClient(TestService, t);

        const msg = await ts.clientStream(createAsyncIterable([
            create(MsgSchema, { value: 3 }),
            create(MsgSchema, { value: 2 }),
            create(MsgSchema, { value: 1 }),
        ]));

        expect(msg.value).toBe(6);

        await rpcs.disconnect();
    });

    it("performs bidir stream", async () => {
        const rpcs = new WebsocketRpcs(e2e_test_addr);
        await rpcs.connect();

        const t = new GoatTransport(rpcs, { destinationName: "e2e" });
        const ts = createClient(TestService, t);

        const arr = await fromAsync(ts.bidiStream(createAsyncIterable(
            [3, 1, 0].map(x => create(MsgSchema, { value: x })),
        )));

        expect(arr.map(m => m.value)).toStrictEqual([0, 1, 2, 0]);

        await rpcs.disconnect();
    });
});
