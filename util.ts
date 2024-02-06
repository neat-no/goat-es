export class AwaitableQueue<T> {
    private q = Array<T>();
    private notify: (() => void)[] = [];

    push(item: T) {
        this.q.push(item);
        this.notify.forEach(v => v());
        this.notify = [];
    }
    get length(): number {
        return this.q.length;
    }
    popSync(): T | undefined {
        return this.q.shift();
    }
    async pop(): Promise<T> {
        await this.nonEmpty();
        return this.popSync()!;
    }
    async nonEmpty(): Promise<void> {
        while (this.q.length == 0) {
            await new Promise<void>(r => {
                this.notify.push(r);
            });
        }
    }
}
