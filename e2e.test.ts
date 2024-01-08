import { Rpc } from "gen/goatorepo/rpc_pb";
import { GoatTransport, RpcReadWriter } from "goat";
import { AwaitableQueue } from "./util";
import WebSocket from "ws";
import { TestService } from "gen/testproto/test_connect";
import { Msg } from "gen/testproto/test_pb";
import { createPromiseClient } from "@connectrpc/connect";
import { createAsyncIterable } from "@connectrpc/connect/protocol";

var e2e_test_addr = process.env.E2E_TEST_ADDR ?? "ws://localhost:9043/test";

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
                const rpc = new Rpc({}).fromBinary(data as Uint8Array);
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
            this.ws.close();
        });
    }

    async read(): Promise<Rpc> {
        return this.queue.pop();
    }

    async write(rpc: Rpc): Promise<void> {
        this.ws.send(rpc.toBinary());
    }
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

        const t = new GoatTransport(rpcs);
        const ts = createPromiseClient(TestService, t);

        const val = await ts.unary(new Msg({ value: 21 }));
        expect(val.value).toBe(42);
    });

    it("performs server stream", async () => {
        const rpcs = new WebsocketRpcs(e2e_test_addr);
        await rpcs.connect();

        const t = new GoatTransport(rpcs);
        const ts = createPromiseClient(TestService, t);

        const arr = await fromAsync(ts.serverStream(new Msg({ value: 6 })));

        expect(arr.map(m => m.value)).toStrictEqual([0, 1, 2, 3, 4, 5]);
    });

    it("performs client stream", async () => {
        const rpcs = new WebsocketRpcs(e2e_test_addr);
        await rpcs.connect();

        const t = new GoatTransport(rpcs);
        const ts = createPromiseClient(TestService, t);

        const msg = await ts.clientStream(createAsyncIterable([
            new Msg({ value: 3 }),
            new Msg({ value: 2 }),
            new Msg({ value: 1 }),
        ]));

        expect(msg.value).toBe(6);
    });

    it("performs bidir stream", async () => {
        const rpcs = new WebsocketRpcs(e2e_test_addr);
        await rpcs.connect();

        const t = new GoatTransport(rpcs);
        const ts = createPromiseClient(TestService, t);

        const arr = await fromAsync(ts.bidiStream(createAsyncIterable(
            [3, 1, 0].map(x => new Msg({ value: x })),
        )));

        expect(arr.map(m => m.value)).toStrictEqual([0, 1, 2, 0]);
    });
});
