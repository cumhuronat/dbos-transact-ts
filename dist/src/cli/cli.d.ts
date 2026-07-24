#!/usr/bin/env node
import { DBOSConfigInternal } from '../dbos-executor';
import { GlobalLogger } from '../telemetry/logs';
export declare function runAndLog(migrationCommands: string[], config: DBOSConfigInternal, action: (migrationCommands: string[], systemDatabaseUrl: string, logger: GlobalLogger) => Promise<number> | number): Promise<void>;
//# sourceMappingURL=cli.d.ts.map