"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.getClientConfig = exports.interceptStreams = exports.exhaustiveCheckGuard = exports.waitForAbort = exports.interruptibleSleep = exports.Semaphore = exports.cancellableSleep = exports.INTERNAL_QUEUE_NAME = exports.sleepConfig = exports.sleepms = exports.globalParams = exports.defaultEnableOTLP = exports.readFile = void 0;
const node_fs_1 = __importDefault(require("node:fs"));
const node_path_1 = __importDefault(require("node:path"));
/*
 * A wrapper of readFile used for mocking in tests
 **/
async function readFile(path, encoding = 'utf8') {
    return await node_fs_1.default.promises.readFile(path, { encoding });
}
exports.readFile = readFile;
function loadDbosVersion() {
    try {
        function findPackageRoot(start) {
            if (typeof start === 'string') {
                if (!start.endsWith(node_path_1.default.sep)) {
                    start += node_path_1.default.sep;
                }
                start = start.split(node_path_1.default.sep);
            }
            if (start.length === 0) {
                throw new Error('package.json not found in path');
            }
            start.pop();
            const dir = start.join(node_path_1.default.sep);
            if (node_fs_1.default.existsSync(node_path_1.default.join(dir, 'package.json'))) {
                return dir;
            }
            return findPackageRoot(start);
        }
        const packageJsonPath = node_path_1.default.join(findPackageRoot(__dirname), 'package.json');
        // eslint-disable-next-line @typescript-eslint/no-require-imports
        const packageJson = require(packageJsonPath);
        return packageJson.version;
    }
    catch (error) {
        // Return "unknown" if package.json cannot be found or loaded
        // This can happen in bundled environments where the file system structure is different
        return 'unknown';
    }
}
// Enable OTLP by default only in DBOS Cloud. Otherwise, enable through configuration.
function defaultEnableOTLP() {
    return process.env.DBOS__CLOUD === 'true';
}
exports.defaultEnableOTLP = defaultEnableOTLP;
exports.globalParams = {
    appVersion: process.env.DBOS__APPVERSION || '', // The one true source of appVersion
    wasComputed: false, // Was app version set or computed? Stored procs don't support computed versions.
    executorID: process.env.DBOS__VMID || 'local', // The one true source of executorID
    appID: process.env.DBOS__APPID || '', // The one true source of appID
    enableOTLP: defaultEnableOTLP(), // Whether OTLP is enabled
    tracingEnabled: false, // Whether span creation is active (enableOTLP or external TracerProvider)
    dbosVersion: loadDbosVersion(), // The version of the DBOS library
    dbosCloud: process.env.DBOS__CLOUD === 'true', // Whether running in DBOS Cloud
};
const sleepms = (ms) => new Promise((r) => setTimeout(r, ms));
exports.sleepms = sleepms;
// Node.js setTimeout uses a 32-bit signed integer for the delay, so the max is 2^31 - 1 ms (~24.8 days).
exports.sleepConfig = {
    maxTimeoutMS: 2_147_483_647,
};
// The name of the internal queue used by DBOS
exports.INTERNAL_QUEUE_NAME = '_dbos_internal_queue';
/*
A cancellable sleep function that returns a promise and a callback
The promise can be awaited for and will automatically resolve after the given time
When cancel is called, not only it clears the timeout, but also resolves the promise
So any waiters on the cancelable sleep will be resolved
*/
function cancellableSleep(ms) {
    let timeoutId = undefined;
    let resolvePromise;
    let resolved = false;
    const promise = new Promise((resolve) => {
        resolvePromise = () => {
            if (resolved)
                return;
            resolved = true;
            resolve();
            timeoutId = undefined;
        };
        timeoutId = setTimeout(resolvePromise, ms);
    });
    const cancel = () => {
        if (timeoutId) {
            clearTimeout(timeoutId);
            timeoutId = undefined;
            resolvePromise();
        }
    };
    return { promise, cancel };
}
exports.cancellableSleep = cancellableSleep;
/**
 * A minimal counting semaphore used to cap how many operations may run
 * concurrently. Acquirers that arrive while the limit is exhausted queue in
 * FIFO order and are woken as permits are released.
 *
 * A non-positive limit disables the semaphore: acquire resolves immediately and
 * release is a no-op, so the limiter adds no overhead when it is turned off.
 */
class Semaphore {
    available;
    waiters = [];
    enabled;
    constructor(limit) {
        this.enabled = Number.isFinite(limit) && limit > 0;
        this.available = this.enabled ? limit : 0;
    }
    /** Acquire a permit, waiting if necessary. Resolves once a permit is held. */
    acquire() {
        if (!this.enabled)
            return Promise.resolve();
        if (this.available > 0) {
            this.available--;
            return Promise.resolve();
        }
        return new Promise((resolve) => {
            this.waiters.push(resolve);
        });
    }
    /** Release a previously acquired permit, waking the next waiter if any. */
    release() {
        if (!this.enabled)
            return;
        const next = this.waiters.shift();
        if (next) {
            // Hand the permit directly to the next waiter without touching `available`.
            next();
        }
        else {
            this.available++;
        }
    }
    /** Run `fn` while holding a permit, releasing it on every resolution path. */
    async runExclusive(fn) {
        await this.acquire();
        try {
            return await fn();
        }
        finally {
            this.release();
        }
    }
}
exports.Semaphore = Semaphore;
/**
 * Sleep for `ms` milliseconds, resolving early (without throwing) when
 * `signal` aborts. The abort listener is detached on every resolution path,
 * so calling this in a tight loop against a long-lived signal does not
 * accumulate listeners — unlike a `Promise.race([timer, stopPromise])`
 * pattern, which leaks a `.then` handler on each iteration.
 */
function interruptibleSleep(ms, signal) {
    if (signal.aborted)
        return Promise.resolve();
    return new Promise((resolve) => {
        const onAbort = () => {
            clearTimeout(timer);
            resolve();
        };
        const timer = setTimeout(() => {
            signal.removeEventListener('abort', onAbort);
            resolve();
        }, ms);
        signal.addEventListener('abort', onAbort, { once: true });
    });
}
exports.interruptibleSleep = interruptibleSleep;
/** Resolve once `signal` aborts. One-shot listener, auto-removed on fire. */
function waitForAbort(signal) {
    if (signal.aborted)
        return Promise.resolve();
    return new Promise((resolve) => {
        signal.addEventListener('abort', () => resolve(), { once: true });
    });
}
exports.waitForAbort = waitForAbort;
function exhaustiveCheckGuard(_) {
    throw new Error('Exaustive matching is not applied');
}
exports.exhaustiveCheckGuard = exhaustiveCheckGuard;
// Capture original functions
const originalStdoutWrite = process.stdout.write.bind(process.stdout);
const originalStderrWrite = process.stderr.write.bind(process.stderr);
function interceptStreams(onMessage) {
    const intercept = (stream, originalWrite) => {
        return (chunk, encodingOrCb, cb) => {
            const message = chunk.toString();
            onMessage(message, stream);
            if (typeof encodingOrCb === 'function') {
                return originalWrite(chunk, encodingOrCb); // Handle case where encodingOrCb is a callback
            }
            return originalWrite(chunk, encodingOrCb, cb); // Handle case where encodingOrCb is a BufferEncoding
        };
    };
    process.stdout.write = intercept('stdout', originalStdoutWrite);
    process.stderr.write = intercept('stderr', originalStderrWrite);
}
exports.interceptStreams = interceptStreams;
// The `pg` package we use does not parse the connect_timeout parameter, so we need to handle it ourselves.
function getClientConfig(databaseUrl) {
    const connectionString = typeof databaseUrl === 'string' ? databaseUrl : databaseUrl.toString();
    const timeout = getTimeout(typeof databaseUrl === 'string' ? new URL(databaseUrl) : databaseUrl);
    return {
        connectionString,
        connectionTimeoutMillis: timeout ? timeout * 1000 : 10000,
    };
    function getTimeout(url) {
        try {
            const $timeout = url.searchParams.get('connect_timeout');
            return $timeout ? Number.parseInt($timeout, 10) : undefined;
        }
        catch {
            // Ignore errors in parsing the connect_timeout parameter
            return undefined;
        }
    }
}
exports.getClientConfig = getClientConfig;
//# sourceMappingURL=utils.js.map