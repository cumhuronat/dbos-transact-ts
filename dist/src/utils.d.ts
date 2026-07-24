/// <reference types="node" />
export declare function readFile(path: string, encoding?: BufferEncoding): Promise<string>;
export declare function defaultEnableOTLP(): boolean;
export declare const globalParams: {
    appVersion: string;
    wasComputed: boolean;
    executorID: string;
    appID: string;
    enableOTLP: boolean;
    tracingEnabled: boolean;
    dbosVersion: string;
    dbosCloud: boolean;
};
export declare const sleepms: (ms: number) => Promise<unknown>;
export declare const sleepConfig: {
    maxTimeoutMS: number;
};
export declare const INTERNAL_QUEUE_NAME = "_dbos_internal_queue";
export declare function cancellableSleep(ms: number): {
    promise: Promise<void>;
    cancel: () => void;
};
/**
 * A minimal counting semaphore used to cap how many operations may run
 * concurrently. Acquirers that arrive while the limit is exhausted queue in
 * FIFO order and are woken as permits are released.
 *
 * A non-positive limit disables the semaphore: acquire resolves immediately and
 * release is a no-op, so the limiter adds no overhead when it is turned off.
 */
export declare class Semaphore {
    private available;
    private readonly waiters;
    private readonly enabled;
    constructor(limit: number);
    /** Acquire a permit, waiting if necessary. Resolves once a permit is held. */
    acquire(): Promise<void>;
    /** Release a previously acquired permit, waking the next waiter if any. */
    release(): void;
    /** Run `fn` while holding a permit, releasing it on every resolution path. */
    runExclusive<T>(fn: () => Promise<T>): Promise<T>;
}
/**
 * Sleep for `ms` milliseconds, resolving early (without throwing) when
 * `signal` aborts. The abort listener is detached on every resolution path,
 * so calling this in a tight loop against a long-lived signal does not
 * accumulate listeners — unlike a `Promise.race([timer, stopPromise])`
 * pattern, which leaks a `.then` handler on each iteration.
 */
export declare function interruptibleSleep(ms: number, signal: AbortSignal): Promise<void>;
/** Resolve once `signal` aborts. One-shot listener, auto-removed on fire. */
export declare function waitForAbort(signal: AbortSignal): Promise<void>;
export type ValuesOf<T> = T[keyof T];
export declare function exhaustiveCheckGuard(_: never): never;
export declare function interceptStreams(onMessage: (msg: string, stream: 'stdout' | 'stderr') => void): void;
export declare function getClientConfig(databaseUrl: string | URL): {
    connectionString: string;
    connectionTimeoutMillis: number;
};
//# sourceMappingURL=utils.d.ts.map