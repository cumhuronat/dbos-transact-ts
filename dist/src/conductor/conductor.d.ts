/// <reference types="node" />
import { DBOSExecutor } from '../dbos-executor';
import WebSocket from 'ws';
interface IntervalTimeout {
    interval: NodeJS.Timeout | undefined;
    timeout: NodeJS.Timeout | undefined;
}
export declare class Conductor {
    readonly dbosExec: DBOSExecutor;
    readonly appName: string;
    readonly conductorKey: string;
    readonly conductorURL: string;
    readonly executorMetadata?: Record<string, unknown> | undefined;
    url: string;
    websocket: WebSocket | undefined;
    isShuttingDown: boolean;
    isClosed: boolean;
    pingPeriodMs: number;
    pingTimeoutMs: number;
    pingIntervalTimeout: IntervalTimeout | undefined;
    reconnectDelayMs: number;
    reconnectTimeout: NodeJS.Timeout | undefined;
    constructor(dbosExec: DBOSExecutor, appName: string, conductorKey: string, conductorURL: string, executorMetadata?: Record<string, unknown> | undefined);
    resetWebsocket(currWebsocket?: WebSocket, currPing?: IntervalTimeout): void;
    setPingInterval(currWebsocket: WebSocket, currPing: IntervalTimeout): void;
    dispatchLoop(): void;
    stop(): void;
}
export {};
//# sourceMappingURL=conductor.d.ts.map