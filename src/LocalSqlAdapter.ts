import initSqlJs, {Database, SqlJsStatic} from 'sql.js';
class LocalSqlAdapter {

  public rawConnection?: Database;

  constructor(protected options?: { database?: string }) {
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
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const { database } = this.options;
    const SQL: SqlJsStatic = await initSqlJs();
    this.rawConnection = new SQL.Database();
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
    if (this.rawConnection) {
      this.rawConnection.close();
    }
    this.rawConnection = null;
  }

}

export {
  LocalSqlAdapter
}
