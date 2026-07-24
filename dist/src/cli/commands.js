"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.runCommand = void 0;
const child_process_1 = require("child_process");
// Run the "start" command provided by users in their dbos-config.yaml
function runCommand(command, logger) {
    return new Promise((resolve, reject) => {
        // Split the command into the executable and its arguments
        const [executable, ...args] = command.split(' ');
        // Spawn a child process
        const child = (0, child_process_1.spawn)(executable, args, {
            stdio: 'inherit', // Forward stdin, stdout, stderr
            shell: true, // Use the shell to interpret the command
        });
        // Handle parent signals and forward to child
        const handleSignal = (signal) => {
            logger.debug(`Received ${signal}, forwarding to  process...`);
            if (child.pid) {
                process.kill(child.pid, signal);
            }
        };
        process.on('SIGTERM', () => handleSignal('SIGTERM'));
        process.on('SIGINT', () => handleSignal('SIGINT'));
        // Cleanup when child exits
        child.on('exit', (code, signal) => {
            if (code === 0 || signal === 'SIGTERM' || signal === 'SIGINT') {
                logger.info(`Process exited successfully with code ${code ?? 'unknown'} or signal ${signal ?? 'unknown'}`);
                resolve(0);
            }
            else {
                const errorMsg = `Process exited with code ${code ?? 'unknown'} or signal ${signal ?? 'unknown'}`;
                logger.error(errorMsg);
                // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors
                reject(code);
            }
        });
        // Must be caught by the caller
        child.on('error', (error) => {
            logger.error(`Failed to start process: ${error.message}`);
            // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors
            reject(1);
        });
    });
}
exports.runCommand = runCommand;
//# sourceMappingURL=commands.js.map