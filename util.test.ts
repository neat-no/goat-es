import { AwaitableQueue } from "./util";

describe("unit: AwaitableQueue", () => {
    beforeEach(() => {
        vi.useFakeTimers();
    });
    afterEach(() => {
        vi.useRealTimers();
    });

    it("works", async () => {
        const q = new AwaitableQueue<number>();

        expect(q.length).toBe(0);

        // It's empty, so nonEmpty should not resolve immediately
        const nonEmpty1 = await Promise.race([
            q.nonEmpty(),
            Promise.resolve("xyz"),
        ]);
        expect(nonEmpty1).toBe("xyz");

        q.push(1);
        expect(q.length).toBe(1);

        const nonEmpty2 = await Promise.race([
            q.nonEmpty(),
            Promise.resolve("xyz"),
        ]);
        expect(nonEmpty2).toBeUndefined();

        expect(q.length).toBe(1);

        const item1 = q.popSync();
        expect(item1).toBe(1);

        expect(q.length).toBe(0);

        q.push(1);
        q.push(2);
        q.push(3);

        expect(await q.pop()).toBe(1);
        expect(await q.pop()).toBe(2);
        expect(await q.pop()).toBe(3);
    });

    it("handles multiple waiters", async () => {
        const q = new AwaitableQueue<number>();
        var order: number[] = [];

        q.nonEmpty().then(() => {
            order.push(1);
        });
        q.nonEmpty().then(() => {
            order.push(2);
        });

        expect(q.length).toBe(0);
        expect(order).toStrictEqual([]);

        q.push(0);
        await vi.runAllTimersAsync();

        expect(q.length).toBe(1);
        expect(order).toStrictEqual([1, 2]);
    })
});
