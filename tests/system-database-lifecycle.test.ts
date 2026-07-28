import { EventEmitter } from 'node:events';
import type { Pool, PoolClient } from 'pg';
import { DBOSJSON } from '../src/serialization';
import { SystemDatabase } from '../src/system_database';
import { GlobalLogger } from '../src/telemetry/logs';

class LifecyclePool extends EventEmitter {
  readonly options = { max: 1 };
  readonly end = jest.fn(() => Promise.resolve());
}

describe('system database notification lifecycle', () => {
  test('disarms the notification client before releasing it during shutdown', async () => {
    const pool = new LifecyclePool();
    const sysdb = new SystemDatabase('postgres://unused', new GlobalLogger(), DBOSJSON, 1, pool as unknown as Pool);
    const notificationClient = new EventEmitter() as EventEmitter & {
      release: jest.Mock<void, [boolean?]>;
    };
    let released = false;
    notificationClient.release = jest.fn(() => {
      if (released) {
        throw new Error('notification client released twice');
      }
      released = true;
      notificationClient.emit('error', new Error('socket closed during release'));
    });
    const lifecycleErrorHandler = () => notificationClient.release(true);
    notificationClient.on('error', () => undefined);
    notificationClient.on('error', lifecycleErrorHandler);
    sysdb.notificationsClient = notificationClient as unknown as PoolClient;
    (
      sysdb as unknown as {
        notificationsErrorHandler: (error: Error) => void;
      }
    ).notificationsErrorHandler = lifecycleErrorHandler;

    await expect(sysdb.destroy()).resolves.toBeUndefined();

    expect(notificationClient.release).toHaveBeenCalledTimes(1);
    expect(notificationClient.listeners('error')).not.toContain(lifecycleErrorHandler);
    expect(sysdb.notificationsClient).toBeNull();
    expect(pool.end).toHaveBeenCalledTimes(1);
  });
});
