# Typescript / ECMAScript support for GOAT

See [GOAT](https://github.com/neat-no/goat) for the canonical GOAT repository.

This repo provides a Typescript GOAT implementation.

## Using

See the unit tests for examples. In short, `GoatTransport` implements the `Transport` interface defined by [connect-es](https://github.com/connectrpc/connect-es). This means the `GoatTransport` instance can be passed to `createClient()` in lieu of e.g. `createGrpcWebTransport()`.

```typescript
const underlying = {
    read: () => {
        return Promse.reject("TODO: resolve a Rpc here");
    },
    write: (rpc: Rpc) => {
        throw new Error("TODO: write the Rpc here");
    },
};
const transport = new GoatTransport(underlying);
const ts = createClient(TestService, transport);
await ts.unary(create(MsgSchema, { value: i }));
```

There is a sample underlying implementation of WebSockets in `e2e.test.ts`.