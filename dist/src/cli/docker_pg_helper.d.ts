/**
 * Starts a PostgreSQL database in a Docker container.
 *
 * This function checks if Docker is installed, and if so, starts a local PostgreSQL database in a Docker container.
 * It configures the database with default settings and provides connection information upon successful startup.
 *
 * The function uses environment variable PGPASSWORD if available, otherwise  defaults to 'dbos' as the database password.
 *
 * @returns null
 *
 * @throws {Error} If there is an error starting the Docker container or if the PostgreSQL service does not become available within the timeout period
 */
export declare function startDockerPg(): Promise<void>;
/**
 * Stops the Docker Postgres container.
 *
 * @returns {Promise<boolean>} True if the container was successfully stopped, false if it wasn't running
 * @throws {Error} If there was an error stopping the container
 */
export declare function stopDockerPg(): Promise<boolean>;
//# sourceMappingURL=docker_pg_helper.d.ts.map