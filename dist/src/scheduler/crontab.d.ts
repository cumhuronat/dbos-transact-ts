export declare function convertAsterisksToRanges(expressions: string[]): string[];
/**
 * Validates a Cron-Job expression pattern.
 *   Throws on error.
 */
export declare function validateCrontab(pattern: string): string;
/**
 * Validates an IANA timezone string.
 *   Throws on error.
 */
export declare function validateTimezone(timezone: string): void;
export declare function convertExpression(crontab: string): string;
export declare class TimeMatcher {
    #private;
    constructor(pattern: string, timezone?: string);
    match(date: Date | number): boolean;
    nextWakeupTime(date: Date | number): Date;
}
//# sourceMappingURL=crontab.d.ts.map