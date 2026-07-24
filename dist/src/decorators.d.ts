import { WorkflowConfig } from './workflow';
import { StepConfig } from './step';
import { DataSourceTransactionHandler } from './datasource';
/**
 * Alert handler function signature for receiving alerts from DBOS Conductor.
 * @param name - Name/type of the alert
 * @param message - Alert message content
 * @param metadata - Additional key-value metadata
 */
export type AlertHandler = (name: string, message: string, metadata: Record<string, string>) => Promise<void>;
/** @internal */
export declare function getAlertHandler(): AlertHandler | undefined;
/** @internal */
export declare function setAlertHandler(handler: AlertHandler): void;
export type TypedAsyncFunction<T extends unknown[], R> = (...args: T) => Promise<R>;
export type UntypedAsyncFunction = TypedAsyncFunction<unknown[], unknown>;
/**
 * Interface for naming DBOS-registered functions.
 *   These names are used for log and database entries.
 *   They are used for lookup in some cases (like workflow recovery)
 */
export interface FunctionName {
    /** Function name; if not provided, this will be taken from the function's `name` */
    name?: string;
    /** Class name; if not provided, the class constructor or prototype's `name` will be used, or blank otherwise */
    className?: string;
    /**
     * For member functions, class constructor (for `static` methods) or prototype (for instance methods)
     *   This will be used to get the class name if `className` is not provided.
     */
    ctorOrProto?: object;
}
/**
 * Interface for integrating into the DBOS startup/shutdown lifecycle
 */
export interface DBOSLifecycleCallback {
    /** Called back during DBOS launch */
    initialize?(): Promise<void>;
    /** Called back upon shutdown (usually in tests) to close connections and free resources */
    destroy?(): Promise<void>;
    /** Called at launch; Implementers should emit a diagnostic list of all registrations */
    logRegisteredEndpoints?(): void;
}
export interface DBOSMethodMiddlewareInstaller {
    installMiddleware(methodReg: MethodRegistrationBase): void;
}
/**
 * Any column type column can be.
 */
export type DBOSFieldType = 'integer' | 'double' | 'decimal' | 'timestamp' | 'text' | 'varchar' | 'boolean' | 'uuid' | 'json';
export declare class DBOSDataType {
    dataType: DBOSFieldType;
    length: number;
    precision: number;
    scale: number;
    /** Varchar has length */
    static varchar(length: number): DBOSDataType;
    /** Some decimal has precision / scale (as opposed to floating point decimal) */
    static decimal(precision: number, scale: number): DBOSDataType;
    /** Take type from reflect metadata */
    static fromArg(arg?: Function): DBOSDataType | undefined;
    formatAsString(): string;
}
export interface ArgDataType {
    dataType?: DBOSDataType;
}
export declare class MethodParameter {
    name: string;
    index: number;
    externalRegInfo: Map<AnyConstructor | object | string, object>;
    getRegisteredInfo(reg: AnyConstructor | object | string): object;
    get dataType(): DBOSDataType | undefined;
    initializeBaseType(at?: Function): void;
    constructor(idx: number, at?: Function);
}
export declare const DBOS_AUTH = "auth";
export interface ClassAuthDefaults {
    requiredRole?: string[] | undefined;
}
export interface MethodAuth {
    requiredRole?: string[] | undefined;
}
export interface RegistrationDefaults {
    name: string;
    getRegisteredInfo(reg: AnyConstructor | object | string): unknown;
    externalRegInfo: Map<AnyConstructor | object | string, unknown>;
}
export interface MethodRegistrationBase {
    name: string;
    className: string;
    args: MethodParameter[];
    defaults?: RegistrationDefaults;
    getRequiredRoles(): string[];
    workflowConfig?: WorkflowConfig;
    stepConfig?: StepConfig;
    isInstance: boolean;
    externalRegInfo: Map<AnyConstructor | object | string, unknown>;
    wrappedFunction: Function | undefined;
    registeredFunction: Function | undefined;
    origFunction: Function;
    addEntryInterceptor(func: (reg: MethodRegistrationBase, args: unknown[]) => unknown[], seqNum?: number): void;
    getRegisteredInfo(reg: AnyConstructor | object | string): unknown;
    invoke(pthis: unknown, args: unknown[]): unknown;
}
export declare class MethodRegistration<This, Args extends unknown[], Return> implements MethodRegistrationBase {
    defaults?: RegistrationDefaults | undefined;
    name: string;
    className: string;
    classReg: ClassRegistration;
    onEnter: {
        seqNum: number;
        func: (reg: MethodRegistrationBase, args: unknown[]) => unknown[];
    }[];
    addEntryInterceptor(func: (reg: MethodRegistrationBase, args: unknown[]) => unknown[], seqNum?: number): void;
    args: MethodParameter[];
    constructor(classReg: ClassRegistration, origFunc: (this: This, ...args: Args) => Promise<Return>, isInstance: boolean);
    needInitialized: boolean;
    isInstance: boolean;
    origFunction: (this: This, ...args: Args) => Promise<Return>;
    registeredFunction: ((this: This, ...args: Args) => Promise<Return>) | undefined;
    wrappedFunction: ((this: This, ...args: Args) => Promise<Return>) | undefined;
    workflowConfig?: WorkflowConfig;
    stepConfig?: StepConfig;
    regLocation?: string[];
    externalRegInfo: Map<AnyConstructor | object | string, unknown>;
    getRegisteredInfo(reg: AnyConstructor | object | string): {};
    getAssignedType(): 'Workflow' | 'Step' | undefined;
    getClassName(): string;
    checkFuncTypeUnassigned(newType: string): void;
    setStepConfig(stepCfg: StepConfig): void;
    setWorkflowConfig(wfCfg: WorkflowConfig): void;
    init: boolean;
    invoke(pthis: This, args: Args): Promise<Return>;
    getRequiredRoles(): string[];
}
export declare abstract class ConfiguredInstance {
    readonly name: string;
    constructor(name: string);
    /**
     * Override this method to perform async initialization between construction and `DBOS.launch()`.
     */
    initialize(): Promise<void>;
}
export declare class ClassRegistration implements RegistrationDefaults {
    name: string;
    needsInitialized: boolean;
    ormEntities: Function[] | {
        [key: string]: object;
    };
    registeredOperationsByName: Map<string, MethodRegistrationBase>;
    allRegisteredOperations: Map<unknown, MethodRegistrationBase>;
    configuredInstances: Map<string, ConfiguredInstance>;
    configuredInstanceRegLocs: Map<string, string[]>;
    externalRegInfo: Map<AnyConstructor | object | string, unknown>;
    ctor: AnyConstructor | undefined;
    constructor(ctor: AnyConstructor | undefined);
    getClassName(): string;
    registerOperationByName(name: string, reg: MethodRegistrationBase): void;
    getRegisteredInfo(reg: AnyConstructor | object | string): {};
}
export declare function recordDBOSLaunch(): void;
export declare function recordDBOSShutdown(): void;
export declare function ensureDBOSIsNotLaunched(): void;
export declare function ensureDBOSIsLaunched(reason: string): void;
export declare function clearAllRegistrations(): void;
export declare function registerLifecycleCallback(lcl: DBOSLifecycleCallback): void;
export declare function getLifecycleListeners(): readonly DBOSLifecycleCallback[];
export declare function registerMiddlewareInstaller(i: DBOSMethodMiddlewareInstaller): void;
export declare function insertAllMiddleware(): void;
export declare function getRegisteredFunctionFullName(func: unknown): {
    className: string;
    name: string;
};
export declare function getRegisteredFunctionQualifiedName(func: unknown): string;
export declare function getRegisteredFunctionClassName(func: unknown): string;
export declare function getRegisteredFunctionName(func: unknown): string;
export declare function registerFunctionWrapper<This, Args extends unknown[], Return>(func: (this: This, ...args: Args) => Promise<Return>, reg: MethodRegistration<This, Args, Return>): void;
export declare function getFunctionRegistration(func: unknown): MethodRegistration<unknown, unknown[], unknown> | undefined;
export declare function getFunctionRegistrationByName(className: string, name: string): MethodRegistrationBase | undefined;
export declare function getRegisteredOperations(target: object): ReadonlyArray<MethodRegistrationBase>;
export declare function getOrCreateMethodArgsRegistration(target: object | undefined, funcName: PropertyKey, origFunc?: (...args: unknown[]) => unknown): MethodParameter[];
export declare function wrapDBOSFunctionAndRegisterByTarget<This, Args extends unknown[], Return>(target: object, propertyKey: PropertyKey, name: string | undefined, descriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>): {
    descriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>;
    registration: MethodRegistration<This, Args, Return>;
};
export declare function wrapDBOSFunctionAndRegisterByUniqueName<This, Args extends unknown[], Return>(ctorOrProto: object | undefined, className: string | undefined, propertyKey: PropertyKey, name: string | undefined, func: (this: This, ...args: Args) => Promise<Return>): MethodRegistration<This, Args, Return>;
export declare function wrapDBOSFunctionAndRegisterDec<This, Args extends unknown[], Return>(target: object, propertyKey: PropertyKey, name: string | undefined, descriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>): {
    descriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>;
    registration: MethodRegistration<This, Args, Return>;
};
export declare function wrapDBOSFunctionAndRegister<This, Args extends unknown[], Return>(ctorOrProto: object | undefined, className: string | undefined, propertyKey: PropertyKey, name: string | undefined, func: (this: This, ...args: Args) => Promise<Return>): MethodRegistration<This, Args, Return>;
type AnyConstructor = new (...args: unknown[]) => object;
export declare function getNameForClass(ctor: object): string;
export declare function getClassRegistration(target: object, create: boolean): {
    regTarget: AnyConstructor;
    reg: {
        name: string;
        reg: ClassRegistration;
        regloc: string[];
    };
} | {
    regTarget: AnyConstructor;
    reg?: undefined;
};
export declare function getAllRegisteredClassNames(): string[];
export declare function getAllRegisteredFunctions(): MethodRegistrationBase[];
export declare function getClassRegistrationByName(name: string, create?: boolean): ClassRegistration;
export declare function getOrCreateClassRegistrationByTarget<CT extends {
    new (...args: unknown[]): object;
}>(ctor: CT): ClassRegistration;
export declare function getConfiguredInstance(clsname: string, cfgname: string): ConfiguredInstance | null;
export declare function finalizeClassRegistrations(): void;
export declare const transactionalDataSources: Map<string, DataSourceTransactionHandler>;
export declare function registerTransactionalDataSource(name: string, ds: DataSourceTransactionHandler): void;
export declare function getTransactionalDataSource(name: string): DataSourceTransactionHandler;
export declare function associateClassWithExternal(external: AnyConstructor | object | string, cls: AnyConstructor | string): object;
export declare function associateMethodWithExternal<This, Args extends unknown[], Return>(external: AnyConstructor | object | string, target: object | undefined, className: string | undefined, funcName: string, func: (this: This, ...args: Args) => Promise<Return>): {
    registration: MethodRegistration<This, Args, Return>;
    regInfo: object;
};
export declare function associateParameterWithExternal<This, Args extends unknown[], Return>(external: AnyConstructor | object | string, target: object | undefined, className: string | undefined, funcName: string, func: ((this: This, ...args: Args) => Promise<Return>) | undefined, paramId: number | string): object | undefined;
export interface ExternalRegistration {
    classConfig?: unknown;
    methodConfig?: unknown;
    paramConfig: {
        name: string;
        index: number;
        paramConfig?: object;
    }[];
    methodReg: MethodRegistrationBase;
}
export declare function getRegistrationsForExternal(external: AnyConstructor | object | string, cls?: object | string, funcName?: string): readonly ExternalRegistration[];
export declare function ArgName(name: string): (target: object, propertyKey: PropertyKey, parameterIndex: number) => void;
export {};
//# sourceMappingURL=decorators.d.ts.map