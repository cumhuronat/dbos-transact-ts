"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.PortableWorkflowError = void 0;
class PortableWorkflowError extends Error {
    name;
    code;
    data;
    constructor(message, name, code, data) {
        super(message);
        this.name = name;
        this.code = code;
        this.data = data;
    }
}
exports.PortableWorkflowError = PortableWorkflowError;
//# sourceMappingURL=system_db_schema.js.map