"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getPGDatabaseOwner = exports.currentDBUserIdentity = exports.connectToPGAndReportOutcome = exports.connectToPGDatabase = exports.maskDatabaseUrl = exports.getDatabaseNameFromUrl = exports.getPGClientConfig = exports.deriveDatabaseUrl = exports.ensurePGDatabase = exports.dropPGDatabase = void 0;
const pg_1 = require("pg");
/**
 * Drop a postgres database from a postgres server.  This requires a target DB name,
 *  and a way to connect to its server with privileges to issue the drop.  See `opts`.
 * Environment variables are not currently considered.
 *
 * @param opts - Options for connecting to DB and issuing the drop
 * @returns `DropDatabaseResult` indicating success, failures, and any notes or hints
 */
async function dropPGDatabase(opts = {}) {
    if (!opts.urlToDrop && !opts.dbToDrop) {
        throw new TypeError(`dropPGDatabase requires a target database name or URL to DROP`);
    }
    const notes = [];
    const log = (msg) => {
        notes.push(msg);
        (opts.logger ?? console.log)(msg);
    };
    function fail(msg, hint) {
        log(`FAIL: ${msg}${hint ? ` | HINT: ${hint}` : ''}`);
        return { message: msg, status: 'failed', notes, hint };
    }
    const adminUrl = opts.adminUrl ?? (opts.urlToDrop ? deriveDatabaseUrl(opts.urlToDrop, 'postgres') : undefined);
    if (!adminUrl) {
        // We could consider the environment, but let's not right now.
        throw new TypeError(`dropPostgresDatabase requires a connection string to a database with permission to perform the DROP`);
    }
    const maybeTemplate1Url = deriveDatabaseUrl(opts.urlToDrop ?? adminUrl, 'template1');
    const tryTemplate1Fallback = opts.tryTemplate1Fallback ?? true;
    const targetDb = opts.dbToDrop ?? getDatabaseNameFromUrl(opts.urlToDrop);
    if (!targetDb) {
        return fail('Target URL has no database name in the path (e.g., /mydb).', 'Fix the target URL and retry.');
    }
    log(`Target DB to drop: ${targetDb}`);
    log(`Admin URL (planned): ${maskDatabaseUrl(adminUrl)}`);
    // 1) Try admin connection first (best detection for existence & privileges)
    let admin = await connectToPGDatabase(adminUrl, log);
    if (!admin && tryTemplate1Fallback) {
        log(`Admin connect failed. Trying template1 as a fallback...`);
        admin = await connectToPGDatabase(maybeTemplate1Url, log);
    }
    // Helper to check DB existence via catalog (requires admin connection)
    const checkExistsViaAdmin = async () => {
        if (!admin)
            return 'unknown';
        const { rows } = await admin.query(`SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1) AS exists`, [targetDb]);
        return rows[0]?.exists ?? false;
    };
    try {
        // If admin connected, see if the DB exists; if not, this is an early success.
        let exists = 'unknown';
        if (admin) {
            exists = await checkExistsViaAdmin();
            if (exists === false) {
                log(`DB "${targetDb}" does not exist (confirmed via catalog).`);
                return { status: 'did_not_exist', notes, message: 'Success (already dropped)' };
            }
        }
        // If we couldn't connect as admin, try connecting to target to distinguish "doesn't exist" from failure to connect.
        if (!admin) {
            const probe = await connectToPGAndReportOutcome(opts.urlToDrop ?? deriveDatabaseUrl(adminUrl, targetDb), log, 'probe target (existence test)');
            if (probe.result === 'ok') {
                // We can reach the target DB—so it exists—but we’re connected *to* it; we cannot DROP from within.
                await probe.client.end().catch(() => { });
                return fail(`Database "${targetDb}" exists, but we could not establish an admin connection to drop it.`, `Provide an admin/alternate DB URL (same server) with privileges to DROP DATABASE`);
            }
            else if (probe.code === '3D000') {
                log(`DB "${targetDb}" does not exist (error 3D000 while connecting). Database already does not exist.`);
                return { status: 'did_not_exist', notes, message: 'Success (already dropped)' };
            }
            else {
                // Ambiguous: not proven missing, no admin path to check or drop.
                return fail(`Could not establish any admin connection, and target connect failed with ${probe.code ?? probe.result}.`, networkOrAuthHint(probe.code));
            }
        }
        // 2) We have an admin connection and the DB likely exists. Check privileges upfront (nice error).
        const who = await currentDBUserIdentity(admin);
        const owner = await getPGDatabaseOwner(admin, targetDb);
        if (!who.isSuperuser && owner && owner !== who.user) {
            log(`Ownership check: DB owned by "${owner}", current_user is "${who.user}" (superuser=${who.isSuperuser}).`);
            // We can still try (maybe you have sufficient rights via membership), but warn early.
            log(`Warning: You might lack privileges to DROP this database unless you are the owner or superuser.`);
        }
        // 3) Attempt the drop
        try {
            log(`Attempting DROP ... WITH (FORCE).`);
            await dropWithForce(admin, targetDb);
        }
        catch (err) {
            const e = err;
            // If FORCE path failed due to syntax (older server), fallback once.
            if (isForceSyntaxError(e)) {
                log(`WITH (FORCE) not supported by server (syntax error). Falling back to terminate-and-drop.`);
                try {
                    await terminateAndDrop(admin, targetDb, 3000, log);
                }
                catch (err2) {
                    const e2 = err2;
                    await admin.end().catch(() => { });
                    return fail(`Drop failed even after fallback: ${shortenErr(e2)}`, createDropHintFromSqlState(e2?.code));
                }
            }
            else {
                await admin.end().catch(() => { });
                return fail(`Drop failed: ${shortenErr(e)}`, createDropHintFromSqlState(e?.code));
            }
        }
        // 4) Verify postcondition
        const finalExists = await checkExistsViaAdmin();
        if (finalExists === false) {
            log(`Verified: database "${targetDb}" is gone.`);
            return { status: 'dropped', notes, message: 'Success (dropped)' };
        }
        else if (finalExists === true) {
            return fail(`After drop attempt, database "${targetDb}" still exists.`, `Terminate all sessions and retry.`);
        }
        else {
            // Unknown (shouldn't happen with admin connected)
            log(`Could not verify drop due to unexpected state.`);
            return { status: 'dropped', notes, message: 'Success (dropped)' }; // we did our best; treat as success if we didn't see errors
        }
    }
    finally {
        await admin?.end().catch(() => { });
    }
}
exports.dropPGDatabase = dropPGDatabase;
async function ensurePGDatabase(opts) {
    if (!opts.urlToEnsure && !opts.dbToEnsure) {
        throw new TypeError(`ensurePGDatabase requires a target database name or URL to check`);
    }
    const notes = [];
    const log = (msg) => {
        notes.push(msg);
        (opts.logger ?? console.log)(msg);
    };
    function fail(msg, hint) {
        log(`FAIL: ${msg}${hint ? ` | HINT: ${hint}` : ''}`);
        return { status: 'failed', notes, hint, message: msg };
    }
    const targetDb = opts.dbToEnsure ?? getDatabaseNameFromUrl(opts.urlToEnsure);
    if (!targetDb) {
        return fail('Target URL has no database name in the path (e.g., /mydb).', 'Fix the target URL and retry.');
    }
    // Try a quick connect attempt first; this requires the least assumptions and has the least chance of messing us up.
    if (opts.urlToEnsure) {
        try {
            const probe = await connectToPGAndReportOutcome(opts.urlToEnsure, log, 'probe target (existence test)');
            if (probe.result === 'ok') {
                // CockroachDB lets you connect to (and SELECT 1 from) a nonexistent database;
                // hit a catalog that requires the current database to exist to verify.
                try {
                    await probe.client.query('SELECT 1 FROM information_schema.schemata LIMIT 1');
                    await probe.client.end().catch(() => { });
                    return { status: 'already_exists', notes, message: 'Success (already existed)' };
                }
                catch (qerr) {
                    await probe.client.end().catch(() => { });
                    log(`Probe connect succeeded but verification query failed: ${qerr.message}; attempting create.`);
                }
            }
        }
        catch (e) {
            log(`Caught error probing database: (e as Error).message; attempting create.`);
        }
    }
    // At this point, we know we need an admin URL, if only as a base of the real URL
    const adminUrl = opts.adminUrl ?? (opts.urlToEnsure ? deriveDatabaseUrl(opts.urlToEnsure, 'postgres') : undefined);
    if (!adminUrl) {
        // We could consider the environment, but let's not right now.
        throw new TypeError(`dropPostgresDatabase requires a connection string to a database with permission to perform the DROP`);
    }
    const maybeTemplate1Url = deriveDatabaseUrl(opts.urlToEnsure ?? adminUrl, 'template1');
    const tryTemplate1Fallback = opts.tryTemplate1Fallback ?? true;
    // 1) Try admin connection first (best detection for existence & privileges)
    let admin = await connectToPGDatabase(adminUrl, log);
    if (!admin && tryTemplate1Fallback) {
        log(`Admin connect failed. Trying template1 as a fallback...`);
        admin = await connectToPGDatabase(maybeTemplate1Url, log);
    }
    // Helper to check DB existence via catalog (requires admin connection)
    const checkExistsViaAdmin = async () => {
        if (!admin)
            return 'unknown';
        const { rows } = await admin.query(`SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1) AS exists`, [targetDb]);
        return rows[0]?.exists ?? false;
    };
    try {
        // If admin connected, see if the DB exists; if so, this is an early success.
        let exists = 'unknown';
        if (admin) {
            exists = await checkExistsViaAdmin();
            if (exists === true) {
                log(`DB "${targetDb}" exists (confirmed via catalog).`);
                return { status: 'already_exists', notes, message: 'Success (already existed)' };
            }
        }
        // If we couldn't connect as admin, try connecting to target to distinguish "doesn't exist" from failure to connect.
        if (!admin) {
            const dbUrl = opts.urlToEnsure ?? deriveDatabaseUrl(adminUrl, targetDb);
            const probe = await connectToPGAndReportOutcome(dbUrl, log, 'probe target (existence test)');
            if (probe.result === 'ok') {
                // We can reach the target DB.... via a URL derived from admin
                await probe.client.end().catch(() => { });
                log(`Probe of database ${targetDb} via ${maskDatabaseUrl(dbUrl)} succeeds.`);
                return { status: 'already_exists', notes, message: 'Success (already existed)' };
            }
            else {
                // Ambiguous: We do not know it to be there, and we can't make an admin connection to proceed.
                return fail(`Could not establish any admin connection, and target connect failed with ${probe.code ?? probe.result}.`, networkOrAuthHint(probe.code));
            }
        }
        // 3) Attempt the CREATE
        try {
            log(`Attempting CREATE.`);
            await createDb(admin, targetDb);
        }
        catch (err) {
            const e = err;
            await admin.end().catch(() => { });
            return fail(`Create failed: ${shortenErr(e)}`, createDropHintFromSqlState(e?.code));
        }
        // 4) Verify postcondition
        const finalExists = await checkExistsViaAdmin();
        if (finalExists === true) {
            log(`Verified: database "${targetDb}" exists.`);
            return { status: 'created', notes, message: 'Success (created)' };
        }
        else if (finalExists === false) {
            return fail(`After create attempt, database "${targetDb}" does not exist still.`);
        }
        else {
            // Unknown (shouldn't happen with admin connected)
            log(`Could not verify creation due to unexpected state.`);
            return { status: 'created', notes, message: 'Success (unverified)' };
        }
    }
    finally {
        await admin?.end().catch(() => { });
    }
}
exports.ensurePGDatabase = ensurePGDatabase;
function deriveDatabaseUrl(urlStr, otherDbName) {
    try {
        const u = new URL(urlStr);
        u.pathname = `/${otherDbName}`;
        return u.toString();
    }
    catch {
        return urlStr; // best effort; connect will fail with clear message
    }
}
exports.deriveDatabaseUrl = deriveDatabaseUrl;
// The `pg` package we use does not parse the connect_timeout parameter, so we need to handle it ourselves.
function getPGClientConfig(databaseUrl) {
    const connectionString = typeof databaseUrl === 'string' ? databaseUrl : databaseUrl.toString();
    const timeout = getTimeout(typeof databaseUrl === 'string' ? new URL(databaseUrl) : databaseUrl);
    return {
        connectionString,
        connectionTimeoutMillis: timeout ? timeout * 1000 : 10000,
    };
    function getTimeout(url) {
        try {
            const $timeout = url.searchParams.get('connect_timeout');
            return $timeout ? parseInt($timeout, 10) : undefined;
        }
        catch {
            // Ignore errors in parsing the connect_timeout parameter
            return undefined;
        }
    }
}
exports.getPGClientConfig = getPGClientConfig;
function getDatabaseNameFromUrl(urlStr) {
    const u = new URL(urlStr);
    return u.pathname?.replace(/^\//, '') || '';
}
exports.getDatabaseNameFromUrl = getDatabaseNameFromUrl;
function maskDatabaseUrl(urlStr) {
    try {
        const u = new URL(urlStr);
        if (u.password) {
            u.password = encodeURIComponent('***');
        }
        return u.toString();
    }
    catch {
        return urlStr;
    }
}
exports.maskDatabaseUrl = maskDatabaseUrl;
async function connectToPGDatabase(url, log) {
    log(`Connecting: ${maskDatabaseUrl(url)}`);
    const client = new pg_1.Client(getPGClientConfig(url));
    client.on('error', (err) => {
        log(`Unexpected error in startup client: ${err}`);
    });
    try {
        await client.connect();
        return client;
    }
    catch (err) {
        const e = err;
        log(`Connect failed: ${shortenErr(e)}${e?.code ? ` (code ${e.code})` : ''}`);
        try {
            await client.end();
        }
        catch { }
        return null;
    }
}
exports.connectToPGDatabase = connectToPGDatabase;
async function connectToPGAndReportOutcome(url, log, label) {
    log(`Connecting to ${label}: ${maskDatabaseUrl(url)}`);
    const client = new pg_1.Client(getPGClientConfig(url));
    client.on('error', (err) => {
        log(`Unexpected error in startup client: ${err}`);
    });
    try {
        await client.connect();
        return { result: 'ok', client };
    }
    catch (err) {
        const e = err;
        try {
            await client.end();
        }
        catch { }
        return { result: 'error', code: e?.code, message: e?.message ?? String(e) };
    }
}
exports.connectToPGAndReportOutcome = connectToPGAndReportOutcome;
function shortenErr(e) {
    const m = e?.message ?? String(e);
    return m.length > 500 ? `${m.slice(0, 500)}…` : m;
}
async function currentDBUserIdentity(client) {
    const { rows: userRows } = await client.query(`SELECT current_user AS user`);
    const user = userRows[0]?.user ?? '';
    const { rows: roleRows } = await client.query(`SELECT rolsuper FROM pg_roles WHERE rolname = current_user`);
    return { user, isSuperuser: !!roleRows[0]?.rolsuper };
}
exports.currentDBUserIdentity = currentDBUserIdentity;
async function getPGDatabaseOwner(admin, dbName) {
    const { rows } = await admin.query(`SELECT r.rolname AS owner
     FROM pg_database d JOIN pg_roles r ON r.oid = d.datdba
     WHERE d.datname = $1`, [dbName]);
    return rows[0]?.owner ?? null;
}
exports.getPGDatabaseOwner = getPGDatabaseOwner;
function quotePGIdentifier(name) {
    return `"${name.replace(/"/g, '""')}"`;
}
async function dropWithForce(admin, dbName) {
    await admin.query(`DROP DATABASE IF EXISTS ${quotePGIdentifier(dbName)} WITH (FORCE)`);
}
async function createDb(admin, dbName) {
    await admin.query(`CREATE DATABASE ${quotePGIdentifier(dbName)}`);
}
function isForceSyntaxError(e) {
    return e?.code === '42601' /* syntax_error */ || /WITH\s*\(\s*FORCE\s*\)/i.test(e?.message ?? '');
}
async function terminateAndDrop(admin, dbName, settleMs, log) {
    // Prevent new connections (best-effort; ignore errors)
    try {
        await admin.query(`ALTER DATABASE ${quotePGIdentifier(dbName)} WITH ALLOW_CONNECTIONS = false`);
    }
    catch (e) {
        log(`ALTER DATABASE ... ALLOW_CONNECTIONS=false failed (continuing): ${shortenErr(e)}`);
    }
    // Terminate existing sessions (best-effort; not all servers expose pg_terminate_backend, e.g. CockroachDB)
    try {
        await admin.query(`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()`, [dbName]);
    }
    catch (e) {
        log(`pg_terminate_backend failed (continuing): ${shortenErr(e)}`);
    }
    if (settleMs > 0) {
        log(`Waiting ${settleMs}ms for backends to terminate...`);
        await new Promise((r) => setTimeout(r, settleMs));
    }
    // Try DROP, and if "being accessed" shows up, retry once after an extra wait
    try {
        await admin.query(`DROP DATABASE IF EXISTS ${quotePGIdentifier(dbName)}`);
    }
    catch (err) {
        const e = err;
        if (e?.code === '55006') {
            log(`DB still "being accessed by other users"; retrying after extra wait...`);
            await new Promise((r) => setTimeout(r, Math.max(1000, settleMs)));
            try {
                await admin.query(`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()`, [dbName]);
            }
            catch (e2) {
                log(`pg_terminate_backend failed (continuing): ${shortenErr(e2)}`);
            }
            await admin.query(`DROP DATABASE IF EXISTS ${quotePGIdentifier(dbName)}`);
        }
        else {
            throw e;
        }
    }
}
function networkOrAuthHint(code) {
    if (!code)
        return;
    switch (code) {
        case 'ECONNREFUSED':
            return 'Server not reachable. Check host/port or firewall.';
        case 'ENOTFOUND':
            return 'Hostname not resolvable. Check DNS/host.';
        case 'ETIMEDOUT':
            return 'Connection timed out. Check network/firewall.';
        case '28P01':
            return 'Invalid password.';
        case '28000':
            return 'Authentication rejected (pg_hba.conf or method).';
        default: {
            if (code.substring(0, 2) === '28')
                return 'Other connection security error';
            return undefined;
        }
    }
}
function createDropHintFromSqlState(code) {
    switch (code?.substring(0, 5)) {
        case '42501':
            return 'Insufficient privilege. You must be the owner or a superuser.';
        case '53300':
            return 'Too many connections to server; free some slots.';
        default:
            break;
    }
    switch (code?.substring(0, 2)) {
        case '42':
            return 'Syntax error or access rule violation.';
        case '3D':
            return 'Target database does not exist (already gone).';
        case '55':
            return 'Database is in use. Terminate sessions or use PG13+ WITH (FORCE).';
        case '53':
            return 'Insufficient resources.';
        default:
            return undefined;
    }
}
//# sourceMappingURL=database_utils.js.map