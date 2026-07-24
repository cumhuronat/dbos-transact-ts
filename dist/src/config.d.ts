import { DBOSConfig, DBOSRuntimeConfig, DBOSConfigInternal } from './dbos-executor';
export declare const dbosConfigFilePath = "dbos-config.yaml";
export interface ConfigFile {
    name?: string;
    language?: string;
    system_database_url?: string;
    system_database_schema_name?: string;
    database?: {
        migrate?: string[];
    };
    telemetry?: {
        logs?: {
            addContextMetadata?: boolean;
            logLevel?: string;
            silent?: boolean;
        };
        OTLPExporter?: {
            logsEndpoint?: string | string[];
            tracesEndpoint?: string | string[];
        };
    };
    use_listen_notify?: boolean;
    runtimeConfig?: Partial<DBOSRuntimeConfig>;
}
export declare function substituteEnvVars(content: string): string;
export declare function readConfigFile(dirPath?: string): Promise<ConfigFile>;
export declare function writeConfigFile(configFile: ConfigFile, configFilePath: string): void;
export declare function isValidDatabaseName(dbName: string): boolean;
export declare function getSystemDatabaseUrl(configFile: Pick<ConfigFile, 'name' | 'system_database_url'>): string;
export declare function getDbosConfig(config: ConfigFile, options?: {
    logLevel?: string;
    forceConsole?: boolean;
}): DBOSConfigInternal;
export declare function translateDbosConfig(options: DBOSConfig, forceConsole?: boolean): DBOSConfigInternal;
export declare function getRuntimeConfig(config: ConfigFile): DBOSRuntimeConfig;
export declare function translateRuntimeConfig(config?: Partial<DBOSRuntimeConfig & DBOSConfig>): DBOSRuntimeConfig;
export declare function overwriteConfigForDBOSCloud(providedDBOSConfig: DBOSConfigInternal, providedRuntimeConfig: DBOSRuntimeConfig, configFile: ConfigFile): [DBOSConfigInternal, DBOSRuntimeConfig];
//# sourceMappingURL=config.d.ts.map