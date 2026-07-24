"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.validateStepConfig = void 0;
const error_1 = require("./error");
/** Validate a step configuration, throwing `DBOSError` if it is invalid. */
function validateStepConfig(config, stepName) {
    const timeoutMS = config.timeoutMS;
    if (timeoutMS !== undefined && (typeof timeoutMS !== 'number' || !Number.isFinite(timeoutMS) || timeoutMS <= 0)) {
        throw new error_1.DBOSError(`Invalid timeoutMS (${timeoutMS}) in configuration of step ${stepName}: must be a positive number`);
    }
}
exports.validateStepConfig = validateStepConfig;
//# sourceMappingURL=step.js.map