export declare function getCallSiteInfo(): {
    fileName: string;
    lineNumber: number;
};
export interface DebugPoint {
    name: string;
    fileName: string;
    lineNumber: number;
    hitCount: number;
}
export declare class DebugAction {
    sleepms?: number;
    awaitEvent?: Promise<void>;
    callback?: () => void;
    asyncCallback?: () => Promise<void>;
}
export declare const pointTriggers: Map<string, DebugAction>;
export declare const pointLocations: Map<string, DebugPoint>;
export declare function debugTriggerPoint(name: string): Promise<void>;
export declare function setDebugTrigger(name: string, action: DebugAction): void;
export declare function clearDebugTriggers(): void;
export declare const DEBUG_TRIGGER_WORKFLOW_QUEUE_START = "DEBUG_TRIGGER_WORKFLOW_QUEUE_START";
export declare const DEBUG_TRIGGER_WORKFLOW_ENQUEUE = "DEBUG_TRIGGER_WORKFLOW_ENQUEUE";
export declare const DEBUG_TRIGGER_STEP_COMMIT = "DEBUG_TRIGGER_STEP_COMMIT";
export declare const DEBUG_TRIGGER_INITWF_COMMIT = "DEBUG_TRIGGER_INITWF_COMMIT";
export declare const DEBUG_TRIGGER_BETWEEN_PARTITION_DISPATCHES = "DEBUG_TRIGGER_BETWEEN_PARTITION_DISPATCHES";
export declare const DEBUG_TRIGGER_FIND_AND_MARK_AFTER_SELECT = "DEBUG_TRIGGER_FIND_AND_MARK_AFTER_SELECT";
//# sourceMappingURL=debugpoint.d.ts.map