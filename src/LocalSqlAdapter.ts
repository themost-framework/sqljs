import initSqlJs, {Database, SqlJsStatic} from 'sql.js';
class LocalSqlAdapter {

  public rawConnection?: Database;

  public static readonly instances = new Map<string, Database>();

  constructor(protected options?: { name?: string, database?: string, buffer?: ArrayLike<number> }) {
  }

  open(callback: (err?: Error) => void): void {
    void this.openAsync().then(() => {
      return callback();
    }).catch((err: Error) => callback(err))
  }

  async openAsync() {
    if (this.rawConnection) {
      return;
    }
    const name = this.options.name || 'local';
    const rawConnection = LocalSqlAdapter.instances.get(name);
    if (rawConnection) {
      this.rawConnection = rawConnection;
      return;
    }
    const SQL: SqlJsStatic = await initSqlJs();
    LocalSqlAdapter.instances.set(name, new SQL.Database(this.options.buffer))
    this.rawConnection = LocalSqlAdapter.instances.get(name);
  }

  close(callback: (err?: Error) => void): void {
    void this.closeAsync().then(() => {
      callback();
    }).catch((err: Error) => {
      console.error('An error occurred while closing database connection', err);
      return callback();
    })
  }

  async closeAsync() {
    this.rawConnection = null;
  }

}

export {
  LocalSqlAdapter
}
