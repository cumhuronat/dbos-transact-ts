"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ArgName = exports.getRegistrationsForExternal = exports.associateParameterWithExternal = exports.associateMethodWithExternal = exports.associateClassWithExternal = exports.getTransactionalDataSource = exports.registerTransactionalDataSource = exports.transactionalDataSources = exports.finalizeClassRegistrations = exports.getConfiguredInstance = exports.getOrCreateClassRegistrationByTarget = exports.getClassRegistrationByName = exports.getAllRegisteredFunctions = exports.getAllRegisteredClassNames = exports.getClassRegistration = exports.getNameForClass = exports.wrapDBOSFunctionAndRegister = exports.wrapDBOSFunctionAndRegisterDec = exports.wrapDBOSFunctionAndRegisterByUniqueName = exports.wrapDBOSFunctionAndRegisterByTarget = exports.getOrCreateMethodArgsRegistration = exports.getRegisteredOperations = exports.getFunctionRegistrationByName = exports.getFunctionRegistration = exports.registerFunctionWrapper = exports.getRegisteredFunctionName = exports.getRegisteredFunctionClassName = exports.getRegisteredFunctionQualifiedName = exports.getRegisteredFunctionFullName = exports.insertAllMiddleware = exports.registerMiddlewareInstaller = exports.getLifecycleListeners = exports.registerLifecycleCallback = exports.clearAllRegistrations = exports.ensureDBOSIsLaunched = exports.ensureDBOSIsNotLaunched = exports.recordDBOSShutdown = exports.recordDBOSLaunch = exports.ClassRegistration = exports.ConfiguredInstance = exports.MethodRegistration = exports.DBOS_AUTH = exports.MethodParameter = exports.DBOSDataType = exports.setAlertHandler = exports.getAlertHandler = void 0;
const step_1 = require("./step");
const error_1 = require("./error");
// Module-level alert handler storage (internal use only)
let alertHandler = undefined;
/** @internal */
function getAlertHandler() {
    return alertHandler;
}
exports.getAlertHandler = getAlertHandler;
/** @internal */
function setAlertHandler(handler) {
    alertHandler = handler;
}
exports.setAlertHandler = setAlertHandler;
class DBOSDataType {
    dataType = 'text';
    length = -1;
    precision = -1;
    scale = -1;
    /** Varchar has length */
    static varchar(length) {
        const dt = new DBOSDataType();
        dt.dataType = 'varchar';
        dt.length = length;
        return dt;
    }
    /** Some decimal has precision / scale (as opposed to floating point decimal) */
    static decimal(precision, scale) {
        const dt = new DBOSDataType();
        dt.dataType = 'decimal';
        dt.precision = precision;
        dt.scale = scale;
        return dt;
    }
    /** Take type from reflect metadata */
    // eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
    static fromArg(arg) {
        if (!arg)
            return undefined;
        const dt = new DBOSDataType();
        if (arg === String) {
            dt.dataType = 'text';
        }
        else if (arg === Date) {
            dt.dataType = 'timestamp';
        }
        else if (arg === Number) {
            dt.dataType = 'double';
        }
        else if (arg === Boolean) {
            dt.dataType = 'boolean';
        }
        else {
            dt.dataType = 'json';
        }
        return dt;
    }
    formatAsString() {
        let rv = this.dataType;
        if (this.dataType === 'varchar' && this.length > 0) {
            rv += `(${this.length})`;
        }
        if (this.dataType === 'decimal' && this.precision > 0) {
            if (this.scale > 0) {
                rv += `(${this.precision},${this.scale})`;
            }
            else {
                rv += `(${this.precision})`;
            }
        }
        return rv;
    }
}
exports.DBOSDataType = DBOSDataType;
// eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
function getArgNames(func) {
    const fstr = func.toString();
    const args = [];
    let currentArgName = '';
    let nestDepth = 0;
    let inSingleQuote = false;
    let inDoubleQuote = false;
    let inBacktick = false;
    let inComment = false;
    let inBlockComment = false;
    let inDefaultValue = false;
    // Extract parameter list from function signature
    const paramStart = fstr.indexOf('(');
    if (paramStart === -1)
        return [];
    const paramStr = fstr.substring(paramStart + 1);
    for (let i = 0; i < paramStr.length; i++) {
        const char = paramStr[i];
        const nextChar = paramStr[i + 1];
        // Handle comments
        if (inBlockComment) {
            if (char === '*' && nextChar === '/') {
                inBlockComment = false;
                i++; // Skip closing '/'
            }
            continue;
        }
        else if (inComment) {
            if (char === '\n')
                inComment = false;
            continue;
        }
        else if (char === '/' && nextChar === '*') {
            inBlockComment = true;
            i++;
            continue;
        }
        else if (char === '/' && nextChar === '/') {
            inComment = true;
            continue;
        }
        if (inComment || inBlockComment)
            continue;
        // Handle quotes (for default values)
        if (char === "'" && !inDoubleQuote && !inBacktick) {
            inSingleQuote = !inSingleQuote;
        }
        else if (char === '"' && !inSingleQuote && !inBacktick) {
            inDoubleQuote = !inDoubleQuote;
        }
        else if (char === '`' && !inSingleQuote && !inDoubleQuote) {
            inBacktick = !inBacktick;
        }
        // Skip anything inside quotes
        if (inSingleQuote || inDoubleQuote || inBacktick) {
            continue;
        }
        // Handle default values
        if (char === '=' && nestDepth === 0) {
            inDefaultValue = true;
            continue;
        }
        // These can mean default values.  Or destructuring (which is a problem)...
        if (char === '(' || char === '{' || char === '[') {
            nestDepth++;
        }
        if (char === ')' || char === '}' || char === ']') {
            if (nestDepth === 0)
                break; // Done
            nestDepth--;
        }
        // Handle rest parameters `...arg`; this is a problem.
        if (char === '.' && nextChar === '.' && paramStr[i + 2] === '.') {
            i += 2; // Skip the other dots
            continue;
        }
        // Handle argument separators (`,`) at depth 0
        if (char === ',' && nestDepth === 0) {
            if (currentArgName.trim()) {
                args.push(currentArgName.trim());
            }
            currentArgName = '';
            inDefaultValue = false;
            continue;
        }
        // Add valid characters to the current argument
        if (!inDefaultValue) {
            currentArgName += char;
        }
    }
    // Push the last argument if it exists
    if (currentArgName.trim()) {
        if (currentArgName.trim()) {
            args.push(currentArgName.trim());
        }
    }
    return args;
}
class MethodParameter {
    name = '';
    index = -1;
    externalRegInfo = new Map();
    getRegisteredInfo(reg) {
        if (!this.externalRegInfo.has(reg)) {
            this.externalRegInfo.set(reg, {});
        }
        return this.externalRegInfo.get(reg);
    }
    get dataType() {
        return this.getRegisteredInfo('type').dataType;
    }
    // eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
    initializeBaseType(at) {
        if (!this.externalRegInfo.has('type')) {
            this.externalRegInfo.set('type', {});
        }
        const adt = this.externalRegInfo.get('type');
        adt.dataType = DBOSDataType.fromArg(at);
    }
    // eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
    constructor(idx, at) {
        this.index = idx;
        this.initializeBaseType(at);
    }
}
exports.MethodParameter = MethodParameter;
exports.DBOS_AUTH = 'auth';
class MethodRegistration {
    defaults;
    name = '';
    className = '';
    classReg;
    // Interceptors
    onEnter = [];
    addEntryInterceptor(func, seqNum = 10) {
        this.onEnter.push({ seqNum, func });
        this.onEnter.sort((a, b) => a.seqNum - b.seqNum);
    }
    args = [];
    constructor(classReg, origFunc, isInstance) {
        this.classReg = classReg;
        this.origFunction = origFunc;
        this.isInstance = isInstance;
    }
    needInitialized = true;
    isInstance;
    origFunction;
    registeredFunction;
    wrappedFunction = undefined;
    workflowConfig;
    stepConfig;
    regLocation;
    externalRegInfo = new Map();
    getRegisteredInfo(reg) {
        if (!this.externalRegInfo.has(reg)) {
            this.externalRegInfo.set(reg, {});
        }
        return this.externalRegInfo.get(reg);
    }
    getAssignedType() {
        if (this.workflowConfig)
            return 'Workflow';
        if (this.stepConfig)
            return 'Step';
        return undefined;
    }
    getClassName() {
        return this.className || this.classReg.getClassName();
    }
    checkFuncTypeUnassigned(newType) {
        const oldType = this.getAssignedType();
        let error = undefined;
        if (oldType && newType !== oldType) {
            error = `Operation (Name: ${this.getClassName()}.${this.name}) is already registered with a conflicting function type: ${oldType} vs. ${newType}`;
        }
        else if (oldType) {
            error = `Operation (Name: ${this.getClassName()}.${this.name}) is already registered.`;
        }
        if (error) {
            if (this.regLocation) {
                error = error + `\nPrior registration occurred at:\n${this.regLocation.join('\n')}`;
            }
            throw new error_1.DBOSConflictingRegistrationError(`${error}`);
        }
        else {
            this.regLocation = new StackGrabber().getCleanStack(3);
        }
    }
    setStepConfig(stepCfg) {
        this.checkFuncTypeUnassigned('Step');
        (0, step_1.validateStepConfig)(stepCfg, `${this.getClassName()}.${this.name}`);
        this.stepConfig = stepCfg;
    }
    setWorkflowConfig(wfCfg) {
        this.checkFuncTypeUnassigned('Workflow');
        this.workflowConfig = wfCfg;
    }
    init = false;
    invoke(pthis, args) {
        const f = this.wrappedFunction ?? this.registeredFunction ?? this.origFunction;
        return f.call(pthis, ...args);
    }
    getRequiredRoles() {
        const rr = this.getRegisteredInfo(exports.DBOS_AUTH);
        if (rr?.requiredRole) {
            return rr.requiredRole;
        }
        const drr = this.defaults?.getRegisteredInfo(exports.DBOS_AUTH);
        return drr?.requiredRole || [];
    }
}
exports.MethodRegistration = MethodRegistration;
class ConfiguredInstance {
    name;
    constructor(name) {
        if (dbosLaunchPoint) {
            console.warn(`ConfiguredInstance '${name}' is being created after DBOS initialization and was not available for recovery.`);
        }
        this.name = name;
        registerClassInstance(this, name);
    }
    /**
     * Override this method to perform async initialization between construction and `DBOS.launch()`.
     */
    initialize() {
        return Promise.resolve();
    }
}
exports.ConfiguredInstance = ConfiguredInstance;
class ClassRegistration {
    name = '';
    needsInitialized = true;
    // eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
    ormEntities = [];
    registeredOperationsByName = new Map();
    allRegisteredOperations = new Map();
    configuredInstances = new Map();
    configuredInstanceRegLocs = new Map();
    externalRegInfo = new Map();
    ctor;
    constructor(ctor) {
        this.ctor = ctor;
    }
    getClassName() {
        return this.name || this.ctor?.name || '';
    }
    registerOperationByName(name, reg) {
        const er = this.registeredOperationsByName.get(name);
        if (er && er !== reg) {
            let error = `Operation (Name: ${this.name}.${name}) is already registered.`;
            if (er.regLocation) {
                error = error + `\nPrior registration occurred at:\n${er.regLocation.join('\n')}`;
            }
            throw new error_1.DBOSConflictingRegistrationError(`${error}`);
        }
        this.registeredOperationsByName.set(name, reg);
        reg.regLocation = new StackGrabber().getCleanStack(3);
    }
    getRegisteredInfo(reg) {
        if (!this.externalRegInfo.has(reg)) {
            this.externalRegInfo.set(reg, {});
        }
        return this.externalRegInfo.get(reg);
    }
}
exports.ClassRegistration = ClassRegistration;
// #endregion
// #region Global registration structures and functions
class StackGrabber extends Error {
    constructor() {
        super('StackGrabber');
        Error.captureStackTrace(this, StackGrabber); // Excludes constructor from the stack
    }
    getCleanStack(frames = 1) {
        return this.stack
            ?.split('\n')
            .slice(frames + 1)
            .map((l) => '>>> ' + l.replace(/^\s*at\s*/, '')); // Remove the first lines
    }
}
// Track if DBOS is launched, and if so, from where
let dbosLaunchPoint = undefined;
function recordDBOSLaunch() {
    dbosLaunchPoint = new StackGrabber().getCleanStack(2); // Remove one for record, one for registerAndWrap...
}
exports.recordDBOSLaunch = recordDBOSLaunch;
function recordDBOSShutdown() {
    dbosLaunchPoint = undefined;
}
exports.recordDBOSShutdown = recordDBOSShutdown;
function ensureDBOSIsNotLaunched() {
    if (dbosLaunchPoint) {
        throw new error_1.DBOSConflictingRegistrationError(`DBOS code is being registered after DBOS.launch().  DBOS was launched from:\n${dbosLaunchPoint.join('\n')}\n`);
    }
}
exports.ensureDBOSIsNotLaunched = ensureDBOSIsNotLaunched;
function ensureDBOSIsLaunched(reason) {
    if (!dbosLaunchPoint) {
        throw new TypeError(`\`DBOS.launch()\` must be called before running ${reason}.`);
    }
}
exports.ensureDBOSIsLaunched = ensureDBOSIsLaunched;
function clearAllRegistrations() {
    lifecycleListeners.length = 0;
    installedMiddleware = false;
    middlewareInstallers.length = 0;
    functionToRegistration.clear();
    methodArgsByFunction.clear();
    classesByName.clear();
    classesByCtor.clear();
    exports.transactionalDataSources.clear();
    alertHandler = undefined;
}
exports.clearAllRegistrations = clearAllRegistrations;
// DBOS launch lifecycle listener
const lifecycleListeners = [];
function registerLifecycleCallback(lcl) {
    if (!lifecycleListeners.includes(lcl))
        lifecycleListeners.push(lcl);
}
exports.registerLifecycleCallback = registerLifecycleCallback;
function getLifecycleListeners() {
    return lifecycleListeners;
}
exports.getLifecycleListeners = getLifecycleListeners;
// Middleware installers - insert middleware in registered functions prior to launch
let installedMiddleware = false;
const middlewareInstallers = [];
function registerMiddlewareInstaller(i) {
    if (installedMiddleware)
        throw new TypeError('Attempt to provide method middleware after insertion was performed');
    if (!middlewareInstallers.includes(i))
        middlewareInstallers.push(i);
}
exports.registerMiddlewareInstaller = registerMiddlewareInstaller;
function insertAllMiddleware() {
    if (installedMiddleware)
        return;
    installedMiddleware = true;
    const regs = getAllClassRegistrations();
    for (const c of regs) {
        for (const f of c.allRegisteredOperations.values()) {
            for (const i of middlewareInstallers) {
                i.installMiddleware(f);
            }
        }
    }
}
exports.insertAllMiddleware = insertAllMiddleware;
// Registration of functions, and classes
const functionToRegistration = new Map();
// Registration of instance, by constructor+name
function registerClassInstance(inst, instname) {
    const creg = getOrCreateClassRegistrationByTarget(inst.constructor);
    if (creg.configuredInstances.has(instname)) {
        throw new error_1.DBOSConflictingRegistrationError(`An instance of class '${inst.constructor.name}' with name '${instname}' was already registered.  Earlier registration occurred at:\n${(creg.configuredInstanceRegLocs.get(instname) ?? []).join('\n')}`);
    }
    creg.configuredInstances.set(instname, inst);
    creg.configuredInstanceRegLocs.set(instname, new StackGrabber().getCleanStack(3) ?? []);
}
function getRegisteredFunctionFullName(func) {
    let className = '';
    let funcName = func.name ?? '';
    if (functionToRegistration.has(func)) {
        const fr = functionToRegistration.get(func);
        className = fr.getClassName();
        funcName = fr.name;
    }
    return { className, name: funcName };
}
exports.getRegisteredFunctionFullName = getRegisteredFunctionFullName;
function getRegisteredFunctionQualifiedName(func) {
    const fn = getRegisteredFunctionFullName(func);
    return fn.className + '.' + fn.name;
}
exports.getRegisteredFunctionQualifiedName = getRegisteredFunctionQualifiedName;
function getRegisteredFunctionClassName(func) {
    return getRegisteredFunctionFullName(func).className;
}
exports.getRegisteredFunctionClassName = getRegisteredFunctionClassName;
function getRegisteredFunctionName(func) {
    return getRegisteredFunctionFullName(func).name;
}
exports.getRegisteredFunctionName = getRegisteredFunctionName;
function registerFunctionWrapper(func, reg) {
    reg.wrappedFunction = func;
    functionToRegistration.set(func, reg);
}
exports.registerFunctionWrapper = registerFunctionWrapper;
function getFunctionRegistration(func) {
    return functionToRegistration.get(func);
}
exports.getFunctionRegistration = getFunctionRegistration;
function getFunctionRegistrationByName(className, name) {
    const clsreg = getClassRegistrationByName(className, false);
    if (!clsreg)
        return undefined;
    const methReg = clsreg.registeredOperationsByName.get(name);
    if (!methReg)
        return undefined;
    return methReg;
}
exports.getFunctionRegistrationByName = getFunctionRegistrationByName;
function getRegisteredOperations(target) {
    const registeredOperations = [];
    if (typeof target === 'function') {
        // Constructor case
        const classReg = getClassRegistration(target, false);
        classReg.reg?.reg?.allRegisteredOperations?.forEach((m) => registeredOperations.push(m));
    }
    else {
        let current = target;
        while (current) {
            // Walk prototype chain
            registeredOperations.push(...getRegisteredOperations(current.constructor));
            current = Object.getPrototypeOf(current);
        }
    }
    return registeredOperations;
}
exports.getRegisteredOperations = getRegisteredOperations;
const methodArgsByFunction = new Map();
function getOrCreateMethodArgsRegistration(target, funcName, origFunc) {
    let regtarget = target;
    if (regtarget && typeof regtarget !== 'function') {
        regtarget = regtarget.constructor;
    }
    if (!origFunc) {
        origFunc = Object.getOwnPropertyDescriptor(target, funcName).value;
    }
    let mParameters = methodArgsByFunction.get(origFunc);
    if (mParameters === undefined) {
        // eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
        let designParamTypes = undefined;
        if (target) {
            function getDesignType(target, key) {
                // eslint-disable-next-line @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-member-access
                const getMetadata = Reflect?.getMetadata;
                if (!getMetadata)
                    return undefined; // polyfill not present
                return getMetadata('design:paramtypes', target, key); // safe to use
            }
            // eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
            designParamTypes = getDesignType(target, funcName);
        }
        if (designParamTypes) {
            mParameters = designParamTypes.map((value, index) => new MethodParameter(index, value));
        }
        else {
            if (origFunc) {
                const argnames = getArgNames(origFunc);
                mParameters = argnames.map((_value, index) => new MethodParameter(index));
            }
            else {
                const descriptor = Object.getOwnPropertyDescriptor(target, funcName);
                // eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
                const argnames = getArgNames(descriptor?.value);
                mParameters = argnames.map((_value, index) => new MethodParameter(index));
            }
        }
        methodArgsByFunction.set(origFunc, mParameters);
    }
    return mParameters;
}
exports.getOrCreateMethodArgsRegistration = getOrCreateMethodArgsRegistration;
function getOrCreateMethodRegistration(target, className, propertyKey, name, func) {
    const { classReg, isInstance } = getOrCreateClassRegistration(target, className);
    const fname = name ?? propertyKey.toString();
    const origFunc = functionToRegistration.get(func)?.origFunction ?? func;
    if (!classReg.allRegisteredOperations.has(origFunc)) {
        const reg = new MethodRegistration(classReg, func, isInstance);
        classReg.allRegisteredOperations.set(func, reg);
    }
    const methReg = classReg.allRegisteredOperations.get(func);
    if (methReg.needInitialized) {
        methReg.needInitialized = false;
        methReg.name = fname;
        methReg.className = classReg.name;
        methReg.defaults = classReg;
        methReg.args = getOrCreateMethodArgsRegistration(target, propertyKey, func);
        const argNames = getArgNames(func);
        methReg.args.forEach((e) => {
            if (!e.name) {
                if (e.index < argNames.length) {
                    e.name = argNames[e.index];
                }
            }
        });
        const wrappedMethod = async function (...rawArgs) {
            let validatedArgs = rawArgs;
            for (const vf of methReg.onEnter) {
                validatedArgs = vf.func(methReg, validatedArgs);
            }
            return methReg.origFunction.call(this, ...validatedArgs);
        };
        Object.defineProperty(wrappedMethod, 'name', {
            value: methReg.name,
        });
        methReg.registeredFunction = wrappedMethod;
        functionToRegistration.set(methReg.registeredFunction, methReg);
        functionToRegistration.set(methReg.origFunction, methReg);
    }
    return methReg;
}
function wrapDBOSFunctionAndRegisterByTarget(target, propertyKey, name, descriptor) {
    if (!descriptor.value) {
        throw Error('Use of decorator when original method is undefined');
    }
    const registration = wrapDBOSFunctionAndRegisterByUniqueName(target, undefined, propertyKey, name, descriptor.value);
    descriptor.value = registration.wrappedFunction ?? registration.registeredFunction;
    return { descriptor, registration };
}
exports.wrapDBOSFunctionAndRegisterByTarget = wrapDBOSFunctionAndRegisterByTarget;
function wrapDBOSFunctionAndRegisterByUniqueName(ctorOrProto, className, propertyKey, name, func) {
    ensureDBOSIsNotLaunched();
    if (!name) {
        name = typeof propertyKey === 'string' ? propertyKey : propertyKey.toString();
    }
    const freg = getFunctionRegistration(func);
    if (freg) {
        const r = getOrCreateClassRegistration(ctorOrProto, className);
        r.classReg.registerOperationByName(name, freg);
        return freg;
    }
    const registration = getOrCreateMethodRegistration(ctorOrProto, className, propertyKey, name, func);
    const r = getOrCreateClassRegistration(ctorOrProto, className);
    r.classReg.registerOperationByName(name, registration);
    return registration;
}
exports.wrapDBOSFunctionAndRegisterByUniqueName = wrapDBOSFunctionAndRegisterByUniqueName;
function wrapDBOSFunctionAndRegisterDec(target, propertyKey, name, descriptor) {
    if (!descriptor.value) {
        throw Error('Use of decorator when original method is undefined');
    }
    const registration = wrapDBOSFunctionAndRegister(target, undefined, propertyKey, name, descriptor.value);
    descriptor.value = registration.wrappedFunction ?? registration.registeredFunction;
    return { descriptor, registration };
}
exports.wrapDBOSFunctionAndRegisterDec = wrapDBOSFunctionAndRegisterDec;
function wrapDBOSFunctionAndRegister(ctorOrProto, className, propertyKey, name, func) {
    ensureDBOSIsNotLaunched();
    const freg = getFunctionRegistration(func);
    if (freg) {
        return freg;
    }
    const registration = getOrCreateMethodRegistration(ctorOrProto, className, propertyKey, name, func);
    return registration;
}
exports.wrapDBOSFunctionAndRegister = wrapDBOSFunctionAndRegister;
const classesByName = new Map();
const classesByCtor = new Map();
function getNameForClass(ctor) {
    const reg = getClassRegistration(ctor, false);
    return reg.reg?.name || reg.regTarget.name;
}
exports.getNameForClass = getNameForClass;
function getAllClassRegistrations() {
    const seen = new Set();
    for (const [_cn, c] of classesByName) {
        seen.add(c.reg);
    }
    for (const [_c, c] of classesByCtor) {
        seen.add(c.reg);
    }
    return seen;
}
function getClassRegistration(target, create) {
    let regTarget;
    if (typeof target === 'function') {
        // Static method case
        regTarget = target;
    }
    else {
        // Instance method case
        regTarget = target.constructor;
    }
    if (classesByCtor.has(regTarget))
        return { regTarget, reg: classesByCtor.get(regTarget) };
    if (!create)
        return { regTarget };
    classesByCtor.set(regTarget, {
        reg: new ClassRegistration(regTarget),
        name: regTarget.name,
        regloc: new StackGrabber().getCleanStack(1) ?? [],
    });
    return { regTarget, reg: classesByCtor.get(regTarget) };
}
exports.getClassRegistration = getClassRegistration;
function getAllRegisteredClassNames() {
    const cnames = [];
    for (const [cn, _creg] of classesByName) {
        cnames.push(cn);
    }
    return cnames;
}
exports.getAllRegisteredClassNames = getAllRegisteredClassNames;
function getAllRegisteredFunctions() {
    const s = new Set();
    const fregs = [];
    for (const [_f, reg] of functionToRegistration) {
        if (s.has(reg))
            continue;
        fregs.push(reg);
        s.add(reg);
    }
    return fregs;
}
exports.getAllRegisteredFunctions = getAllRegisteredFunctions;
function getClassRegistrationByName(name, create = false) {
    if (!classesByName.has(name) && !create) {
        throw new error_1.DBOSNotRegisteredError(name, `Class '${name}' is not registered`);
    }
    if (!classesByName.has(name)) {
        classesByName.set(name, {
            reg: new ClassRegistration(undefined),
            regloc: new StackGrabber().getCleanStack(1) ?? [],
        });
    }
    const clsReg = classesByName.get(name).reg;
    if (clsReg.needsInitialized) {
        clsReg.name = name;
        clsReg.needsInitialized = false;
    }
    return clsReg;
}
exports.getClassRegistrationByName = getClassRegistrationByName;
function getOrCreateClassRegistrationByTarget(ctor) {
    const existing = getClassRegistration(ctor, true);
    const reg = existing.reg.reg;
    // This registration will need initialized... that happens later
    return reg;
}
exports.getOrCreateClassRegistrationByTarget = getOrCreateClassRegistrationByTarget;
function getOrCreateClassRegistration(target, className) {
    if (!target && className === undefined) {
        className = '';
    }
    let regtarget = undefined;
    let isInstance = false;
    if (target) {
        if (typeof target === 'function') {
            // Static method case
            regtarget = target;
        }
        else {
            // Instance method case
            regtarget = target.constructor;
            isInstance = true;
        }
    }
    // If we have no class name, this might get assigned later.  Put in placeholder reg
    if (className === undefined) {
        const reg = getClassRegistration(regtarget, true);
        return { classReg: reg.reg.reg, isInstance, className };
    }
    // If we have no regtarget, this is plain function registration
    if (!regtarget) {
        return { classReg: getClassRegistrationByName(className, true), isInstance: false, className };
    }
    // We have a regtarget and a name ... assign the name
    const reg = getClassRegistration(regtarget, true);
    if (reg.reg.name && reg.reg.name !== className) {
        throw new TypeError(`Attempt to register class under two names: ${reg.reg.name} vs. ${className}`);
    }
    reg.reg.name = className;
    return { classReg: reg.reg.reg, isInstance, className };
}
function getConfiguredInstance(clsname, cfgname) {
    const classReg = getClassRegistrationByName(clsname);
    if (!classReg)
        return null;
    return classReg.configuredInstances.get(cfgname) ?? null;
}
exports.getConfiguredInstance = getConfiguredInstance;
function finalizeClassRegistrations() {
    function setName(reg, cname) {
        reg.name = cname;
        reg.needsInitialized = false;
        for (const [_fn, f] of reg.registeredOperationsByName) {
            f.className = cname;
        }
    }
    for (const [cls, reg] of classesByCtor) {
        const cname = reg.name || reg.reg.name || getNameForClass(cls);
        const ereg = classesByName.get(cname);
        if (!ereg) {
            classesByName.set(cname, { reg: reg.reg, ctor: cls, regloc: reg.regloc });
            reg.name = cname;
            setName(reg.reg, cname);
            continue;
        }
        if (ereg.ctor && ereg.ctor !== cls) {
            throw new error_1.DBOSConflictingRegistrationError(`Class ${cname}(${cls.name}) has been given a name that conflicts with another class ${ereg.ctor?.name}.`);
        }
        if (ereg.reg !== reg.reg) {
            throw new error_1.DBOSConflictingRegistrationError(`Class: ${cname}(${cls.name}) has been given a name that was registered directly by name without a class.`);
        }
        classesByName.set(cname, { reg: reg.reg, ctor: cls, regloc: reg.regloc });
        reg.name = cname;
        setName(reg.reg, cname);
    }
}
exports.finalizeClassRegistrations = finalizeClassRegistrations;
// #endregion
// #region Transactional data source registration
exports.transactionalDataSources = new Map();
// Register data source (user version)
function registerTransactionalDataSource(name, ds) {
    if (exports.transactionalDataSources.has(name)) {
        if (exports.transactionalDataSources.get(name) !== ds) {
            throw new error_1.DBOSConflictingRegistrationError(`Data source with name ${name} is already registered`);
        }
        return;
    }
    ensureDBOSIsNotLaunched();
    exports.transactionalDataSources.set(name, ds);
}
exports.registerTransactionalDataSource = registerTransactionalDataSource;
function getTransactionalDataSource(name) {
    if (exports.transactionalDataSources.has(name))
        return exports.transactionalDataSources.get(name);
    throw new error_1.DBOSNotRegisteredError(name, `Data source '${name}' is not registered`);
}
exports.getTransactionalDataSource = getTransactionalDataSource;
// #endregion
// #region External (event receiver v3)
function associateClassWithExternal(external, cls) {
    let clsreg = undefined;
    if (typeof cls === 'string') {
        clsreg = getClassRegistrationByName(cls, true);
    }
    else {
        clsreg = getClassRegistration(cls, true).reg.reg;
    }
    return clsreg.getRegisteredInfo(external);
}
exports.associateClassWithExternal = associateClassWithExternal;
/*
 * Associates a DBOS function or method with an external class or object.
 *   Likely, this will be invoking or intercepting the method.
 */
function associateMethodWithExternal(external, target, className, funcName, func) {
    const registration = wrapDBOSFunctionAndRegister(target, className, funcName, funcName, func);
    if (!registration.externalRegInfo.has(external)) {
        registration.externalRegInfo.set(external, {});
    }
    return { registration, regInfo: registration.externalRegInfo.get(external) };
}
exports.associateMethodWithExternal = associateMethodWithExternal;
/*
 * Associates a DBOS function or method parameters with an external class or object.
 *   Likely, this will be invoking or intercepting the method.
 */
function associateParameterWithExternal(external, target, className, funcName, func, paramId) {
    if (!func) {
        func = Object.getOwnPropertyDescriptor(target, funcName).value;
    }
    const registration = wrapDBOSFunctionAndRegister(target, className, funcName, funcName, func);
    let param;
    if (typeof paramId === 'number') {
        param = registration.args[paramId];
    }
    else {
        param = registration.args.find((p) => p.name === paramId);
    }
    if (!param)
        return undefined;
    if (!param.externalRegInfo.has(external)) {
        param.externalRegInfo.set(external, {});
    }
    return param.externalRegInfo.get(external);
}
exports.associateParameterWithExternal = associateParameterWithExternal;
function getRegistrationsForExternal(external, cls, funcName) {
    const res = new Array();
    if (cls) {
        let reg = undefined;
        if (typeof cls === 'string') {
            reg = classesByName.get(cls)?.reg;
        }
        else if (typeof cls === 'function') {
            reg = classesByCtor.get(cls)?.reg;
        }
        else if (cls !== undefined && typeof cls === 'object') {
            reg = classesByCtor.get(cls.constructor)?.reg;
        }
        if (reg) {
            if (funcName) {
                const f = reg.registeredOperationsByName.get(funcName);
                if (f) {
                    collectRegForFunction(f);
                }
            }
            else {
                collectRegForClass(reg);
            }
        }
    }
    else {
        const seen = getAllClassRegistrations();
        for (const c of seen) {
            collectRegForClass(c);
        }
    }
    return res;
    function collectRegForClass(reg) {
        for (const f of reg.allRegisteredOperations.values()) {
            collectRegForFunction(f);
        }
    }
    function collectRegForFunction(f) {
        const methodConfig = f.externalRegInfo.get(external);
        const classConfig = f.defaults?.externalRegInfo.get(external);
        const paramConfig = [];
        let hasParamConfig = false;
        for (const arg of f.args) {
            if (arg.externalRegInfo.has(external))
                hasParamConfig = true;
            paramConfig.push({
                name: arg.name,
                index: arg.index,
                paramConfig: arg.externalRegInfo.get(external),
            });
        }
        if (!methodConfig && !classConfig && !hasParamConfig)
            return;
        res.push({ methodReg: f, methodConfig, classConfig: classConfig ?? {}, paramConfig });
    }
}
exports.getRegistrationsForExternal = getRegistrationsForExternal;
// #endregion
// #region Parameter decorators
function ArgName(name) {
    return function (target, propertyKey, parameterIndex) {
        const existingParameters = getOrCreateMethodArgsRegistration(target, propertyKey);
        const curParam = existingParameters[parameterIndex];
        curParam.name = name;
    };
}
exports.ArgName = ArgName;
// #endregion
//# sourceMappingURL=decorators.js.map