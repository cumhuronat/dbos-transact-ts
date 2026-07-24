"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.migrate = void 0;
const child_process_1 = require("child_process");
const system_database_1 = require("../system_database");
async function migrate(migrationCommands, systemDatabaseUrl, logger) {
    let status = 0;
    try {
        migrationCommands?.forEach((cmd) => {
            logger.info(`Executing migration command: ${cmd}`);
            const migrateCommandOutput = (0, child_process_1.execSync)(cmd, { encoding: 'utf-8' });
            console.log(migrateCommandOutput.trimEnd());
        });
    }
    catch (e) {
        logMigrationError(e, logger, 'Error running migration');
        status = 1;
    }
    logger.info('Creating DBOS system database.');
    try {
        await (0, system_database_1.ensureSystemDatabase)(systemDatabaseUrl, logger);
    }
    catch (e) {
        if (e instanceof Error) {
            logger.error(`Error creating DBOS system database: ${e.message}`);
        }
        else {
            logger.error(e);
        }
        status = 1;
    }
    if (status === 0) {
        logger.info('All migration successful!');
    }
    return status;
}
exports.migrate = migrate;
//Test to determine if e can be treated as an ExecSyncError.
function isExecSyncError(e) {
    if (
    //Safeguard against NaN. NaN type is number but NaN !== NaN
    'pid' in e &&
        typeof e.pid === 'number' &&
        e.pid === e.pid &&
        'stdout' in e &&
        (Buffer.isBuffer(e.stdout) || typeof e.stdout === 'string') &&
        'stderr' in e &&
        (Buffer.isBuffer(e.stderr) || typeof e.stderr === 'string')) {
        return true;
    }
    return false;
}
function logMigrationError(e, logger, title) {
    logger.error(title);
    if (e instanceof Error && isExecSyncError(e)) {
        const stderr = e.stderr;
        if (e.stderr.length > 0) {
            logger.error(`Standard Error: ${stderr.toString().trim()}`);
        }
        const stdout = e.stdout;
        if (stdout.length > 0) {
            logger.error(`Standard Output: ${stdout.toString().trim()}`);
        }
        if (e.message) {
            logger.error(e.message);
        }
        if (e.error?.message) {
            logger.error(e.error?.message);
        }
    }
    else {
        logger.error(e);
    }
}
//# sourceMappingURL=migrate.js.map