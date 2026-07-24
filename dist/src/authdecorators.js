"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.registerAuthChecker = void 0;
const context_1 = require("./context");
const dbos_1 = require("./dbos");
const decorators_1 = require("./decorators");
const error_1 = require("./error");
function checkMethodAuth(methReg, args) {
    // Validate the user authentication and populate the role field
    const requiredRoles = methReg.getRequiredRoles();
    if (requiredRoles.length > 0) {
        dbos_1.DBOS.span?.setAttribute('requiredRoles', requiredRoles);
        const curRoles = dbos_1.DBOS.authenticatedRoles;
        let authorized = false;
        const set = new Set(curRoles);
        for (const role of requiredRoles) {
            if (set.has(role)) {
                authorized = true;
                if ((0, context_1.getCurrentContextStore)()) {
                    (0, context_1.getCurrentContextStore)().assumedRole = role;
                }
                break;
            }
        }
        if (!authorized) {
            const err = new error_1.DBOSNotAuthorizedError(`User does not have a role with permission to call ${methReg.name}`, 403);
            dbos_1.DBOS.span?.addEvent('DBOSNotAuthorizedError', { message: err.message });
            throw err;
        }
    }
    return args;
}
class AuthChecker {
    installMiddleware(methReg) {
        const classAuth = methReg?.defaults?.getRegisteredInfo(decorators_1.DBOS_AUTH);
        const methodAuth = methReg?.getRegisteredInfo(decorators_1.DBOS_AUTH);
        const shouldCheck = classAuth?.requiredRole !== undefined || methodAuth?.requiredRole !== undefined;
        if (shouldCheck) {
            methReg.addEntryInterceptor(checkMethodAuth, 10);
        }
    }
}
const authChecker = new AuthChecker();
function registerAuthChecker() {
    (0, decorators_1.registerMiddlewareInstaller)(authChecker);
}
exports.registerAuthChecker = registerAuthChecker;
//# sourceMappingURL=authdecorators.js.map