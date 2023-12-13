import { Rpc } from 'gen/goatorepo/rpc_pb';
import { GoatTransport, RpcReadWriter } from 'goat';
import { AwaitableQueue } from './util';
import WebSocket from 'ws';
import { TestService } from 'gen/testproto/test_connect';
import { Msg } from 'gen/testproto/test_pb';
import { createPromiseClient } from '@connectrpc/connect';


class WebsocketRpcs implements RpcReadWriter {
    private ws = new WebSocket('ws://localhost:9043/test');
    private queue = new AwaitableQueue<Rpc>();

    constructor() {
        this.ws.on('error', console.error);
        
        this.ws.on('message', (data) => {
            try {
                const rpc = new Rpc({}).fromBinary(data as Uint8Array);
                this.queue.push(rpc);
            } catch (err) {
                console.error(err);
            }
        });
    }

    async connect(): Promise<void> {
        await new Promise<void>((res) => {
            this.ws.on('open', res);
        });
    }

    async read(): Promise<Rpc> {
        return this.queue.pop();
    }

    async write(rpc: Rpc): Promise<void> {
        this.ws.send(rpc.toBinary());
    }
}

async function main() {
    const rpcs = new WebsocketRpcs();
    await rpcs.connect();

    const t = new GoatTransport(rpcs);
    const ts = createPromiseClient(TestService, t);
    const val = await ts.unary(new Msg({ value: 21 }));

    console.log(val);
}

main().then(() => {
    console.log("Done")
}).catch((err) => {
    console.error(err)
})