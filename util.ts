export class AwaitableQueue<T> {
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
        return this.q.shift()!;
    }
};