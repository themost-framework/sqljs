import initSqlJs, { Database, SqlJsStatic } from 'sql.js';
import { AsyncSeriesEventEmitter, before, after } from '@themost/events';
import { v4 as uuid4 } from 'uuid';
import MD5 from 'crypto-js/md5';
import isPlainObject from 'lodash/isPlainObject';
import isObjectLike from 'lodash/isObjectLike';
import { QueryExpression, QueryField, SqlUtils } from '@themost/query';
import { LocalSqlFormatter } from './LocalSqlFormatter';
import { eachSeries, waterfall } from 'async';
import { LocalSqlAdapterBase } from './LocalSqlAdapterBase';

declare interface LocalSqlColumn {
  name: string;
  ordinal: number;
  type: string;
  nullable: boolean;
  primary: boolean;
  size?: number;
  scale?: number;
}

declare interface LocalSqlField {
  name: string;
  type: string;
  size?: number;
  scale?: number;
  nullable?: boolean;
  primary?: boolean;
}

declare interface LocalSqlTable {
  create(fields: LocalSqlField[], callback: (err: Error) => void): void;
  createAsync(fields: LocalSqlField[]): Promise<void>;
  add(fields: LocalSqlField[], callback: (err: Error) => void): void;
  addAsync(fields: LocalSqlField[]): Promise<void>;
  change(fields: LocalSqlField[], callback: (err: Error) => void): void;
  changeAsync(fields: LocalSqlField[]): Promise<void>;
  exists(callback: (err: Error, result: boolean) => void): void;
  existsAsync(): Promise<boolean>;
  version(callback: (err: Error, result: string) => void): void;
  versionAsync(): Promise<string>;
  columns(callback: (err: Error, result: LocalSqlColumn[]) => void): void;
  columnsAsync(): Promise<LocalSqlColumn[]>;
}

declare interface LocalSqlIndex {
  name: string;
  columns: string[];
}

declare interface LocalSqlIndexCollection {
  create(name: string, columns: string[], callback: (err: Error, res?: number) => void): void;
  createAsync(name: string, columns: string[]): Promise<void>;
  drop(name: string, callback: (err: Error, res?: number) => void): void;
  dropAsync(name: string): Promise<void>;
  list(callback: (err: Error, res: LocalSqlIndex[]) => void): void;
  listAsync(): Promise<LocalSqlIndex[]>;
}

const GuidRegex = /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/;
const SqlDateRegEx = /^\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}\.\d+\+[0-1][0-9]:[0-5][0-9]$/;

declare interface LocalSqlTableUpgrade {
  add: { name: string, type: string, size?: number, scale?: number, primary?: boolean }[];
  change?: { name: string, type: string, size?: number, scale?: number }[];
  remove?: { name: string, type: string, size?: number, scale?: number }[];
  appliesTo: string;
  model?: string;
  description?: string
  version: string;
  indexes?: { name: string, columns: string[] }[];
  updated?: boolean;
}

declare interface LocalSqlView {
  create(query: QueryExpression | string, callback: (err: Error) => void): void;
  createAsync(query: QueryExpression | string): Promise<void>;
  exists(callback: (err: Error, result: boolean) => void): void;
  existsAsync(): Promise<boolean>;
  drop(callback: (err: Error) => void): void;
  dropAsync(): Promise<void>;
}

// eslint-disable-next-line @typescript-eslint/consistent-indexed-object-style
async function onReceivingJsonObject(event: { target: LocalSqlAdapter, query: string | QueryExpression, params: unknown[], results: { [k: string]: unknown }[] }): Promise<void> {
  if (typeof event.query === 'object' && event.query.$select) {
    // try to identify the usage of a $jsonObject dialect and format result as JSON
    const { $select: select } = event.query;
    if (select) {
      const attrs = Object.keys(select).reduce((previous, current) => {
        const fields = select[current];
        previous.push(...fields);
        return previous;
      }, []).filter((x) => {
        const [key] = Object.keys(x);
        if (typeof key !== 'string') {
          return false;
        }
        return x[key].$jsonObject != null || x[key].$jsonGroupArray != null || x[key].$jsonArray != null;
      }).map((x) => {
        return Object.keys(x)[0];
      });
      if (attrs.length > 0) {
        if (Array.isArray(event.results)) {
          for (const result of event.results) {
            attrs.forEach((attr) => {
              if (Object.prototype.hasOwnProperty.call(result, attr) && typeof result[attr] === 'string') {
                result[attr] = JSON.parse(result[attr]);
              }
            });
          }
        }
      }
    }
  }
}

class LocalSqlAdapter implements LocalSqlAdapterBase {

  public rawConnection?: Database;

  public static readonly instances = new Map<string, Database>();
  public executing: AsyncSeriesEventEmitter<{target: LocalSqlAdapter, query: string | QueryExpression, params: unknown[]}>;
  public executed: AsyncSeriesEventEmitter<{target: LocalSqlAdapter, query: string | QueryExpression, params: unknown[], results?: unknown[]}>;
  public loading: AsyncSeriesEventEmitter<unknown>;
  transaction: boolean;
  public loaded: AsyncSeriesEventEmitter<unknown>;
  
  constructor(protected options?: { name?: string, database?: string, buffer?: ArrayLike<number>, retry?: number, retryInterval?: number }) {
    this.executing = new AsyncSeriesEventEmitter();
    this.executed = new AsyncSeriesEventEmitter();
    this.loading = new AsyncSeriesEventEmitter<{ database?: string, buffer?: Uint8Array }>()
    this.loaded = new AsyncSeriesEventEmitter();
    this.executed.subscribe(onReceivingJsonObject);
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
    const name = (this.options && this.options.name) || 'local';
    const rawConnection = LocalSqlAdapter.instances.get(name);
    if (rawConnection) {
      this.rawConnection = rawConnection;
      return;
    }
    const SQL: SqlJsStatic = await initSqlJs();
    await this.loading.emit(void 0);
    LocalSqlAdapter.instances.set(name, new SQL.Database(this.options?.buffer || new Uint8Array()));
    await this.loaded.emit(void 0);
    this.rawConnection = LocalSqlAdapter.instances.get(name);
    // add custom functions
    // 1. uuid4
    this.rawConnection.create_function('uuid4', () => {
      return uuid4();
    });
    // 2. crypto_md5 
    this.rawConnection.create_function('crypto_md5', (value: unknown) => {
      if (value == null) {
        return null;
      }
      if (typeof value !== 'string') {
        return MD5(value as string).toString();
      }
      if (isObjectLike(value) || isPlainObject(value)) {
        return MD5(JSON.stringify(value)).toString();
      }
      return MD5(value).toString();
    });
    // 3. uuid_str
    // convert uuid string to uuid format
    this.rawConnection.create_function('uuid_str', (value: unknown) => {
      if (value == null) {
        return null;
      }
      if (typeof value !== 'string') {
        const str = value.toString();
        if (GuidRegex.test(str)) {
          return str;
        }
        if (str.length !== 32) {
          throw new Error('Invalid UUID string');
        }
        return [
          str.substring(0, 8),
          str.substring(8, 12),
          str.substring(12, 16),
          str.substring(16, 20),
          str.substring(20, 32)
        ].join('-');
      }
    });
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

  prepare(query: string, params?: unknown[]): string {
    return SqlUtils.format(query, params);
  }

  formatType(field: { type: string, size?: number, scale?: number, nullable?: boolean, primary?: boolean }) {
    const size = field.size || 0;
    let s;
    switch (field.type) {
      case 'Boolean':
        s = 'INTEGER(1,0)';
        break;
      case 'Byte':
        s = 'INTEGER(1,0)';
        break;
      case 'Number':
      case 'Float':
        s = 'REAL';
        break;
      case 'Counter':
        return 'INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL';
      case 'Currency':
        s = 'NUMERIC(' + (field.size || 19) + ',4)';
        break;
      case 'Decimal':
        s = 'NUMERIC';
        if ((field.size) && (field.scale)) {
          s += '(' + field.size + ',' + field.scale + ')';
        }
        break;
      case 'Date':
      case 'DateTime':
        s = 'NUMERIC';
        break;
      case 'Time':
        s = size > 0 ? `TEXT(${size},0)` : 'TEXT';
        break;
      case 'Long':
        s = 'NUMERIC';
        break;
      case 'Duration':
        s = size > 0 ? `TEXT(${size},0)` : 'TEXT(48,0)';
        break;
      case 'Integer':
        s = 'INTEGER' + (field.size ? '(' + field.size + ',0)' : '');
        break;
      case 'URL':
      case 'Text':
      case 'Note':
        s = field.size ? `TEXT(${field.size},0)` : 'TEXT';
        break;
      case 'Image':
      case 'Binary':
        s = 'BLOB';
        break;
      case 'Guid':
        s = 'TEXT(36,0)';
        break;
      case 'Short':
        s = 'INTEGER(2,0)';
        break;
      case 'Json':
        s = 'JSON HIDDEN';
        break;
      default:
        s = 'INTEGER';
        break;
    }
    if (field.primary) {
      return s.concat(' PRIMARY KEY NOT NULL');
    }
    else {
      return s.concat((field.nullable === undefined) ? ' NULL' : (field.nullable ? ' NULL' : ' NOT NULL'));
    }
  }

  executeInTransaction(fn: (callback: (err?: Error) => void) => void, callback: (err?: Error) => void): void {
    this.open((err) => {
      if (err) {
        callback(err);
      } else {
        if (this.transaction) {
          fn((err?: Error) => {
            void callback(err);
          });
        } else {

          const run = (sql: string, executeCallback: (err?: Error) => void) => {
            try {
              this.rawConnection.run(sql);
              return executeCallback(null);
            } catch (executeError) {
              return executeCallback(executeError);
            }
          };

          // set transaction mode before begin
          this.transaction = true;
          //begin transaction
          run('BEGIN TRANSACTION;', (err?: Error) => {
            if (err) {
              // reset transaction mode
              this.transaction = false;
              return callback(err);
            }
            try {
              // invoke method
              fn(function (err) {
                if (err) {
                  // rollback transaction
                  return run('ROLLBACK;', () => {
                    // reset transaction mode on error
                    this.transaction = false;
                    return callback(err);
                  });
                }
                // commit transaction
                run('COMMIT;', (err) => {
                  // reset transaction mode on error
                  this.transaction = false;
                  return callback(err);
                });
              });
            } catch (invokeError) {
              return run('ROLLBACK;', function () {
                // reset transaction mode on error
                this.transaction = false;
                return callback(invokeError);
              });
            }
          });
        }
      }
    });
  }

  executeInTransactionAsync(func: () => Promise<void>): Promise<void> {
    return new Promise((resolve, reject) => {
      return this.executeInTransaction((callback: (err?: Error) => void) => {
        return func().then(() => {
          return callback();
        }).catch(err => {
          return callback(err);
        });
      }, (err) => {
        if (err) {
          return reject(err);
        }
        return resolve(void 0);
      });
    });
  }

  createView(name: string, query: QueryExpression | string, callback: (err?: Error) => void): void {
    this.view(name).create(query, callback);
  }

  format(format: string, obj: { name: string, type: string }): string {
      let result = format;
      if (/%t/.test(format))
          result = result.replace(/%t/g, this.formatType(obj));
      if (/%f/.test(format))
          result = result.replace(/%f/g, obj.name);
      return result;
  }

  migrate(obj: LocalSqlTableUpgrade, callback: (err?: Error) => void): void {
    if (obj == null) {
      return callback();
    }
    const migration = obj;
    // create a copy of columns
    const addColumns = migration.add.slice(0);
    const format = function (format: string, obj: { name: string, type: string }): string {
      let result = format;
      if (/%t/.test(format))
        result = result.replace(/%t/g, this.formatType(obj));
      if (/%f/.test(format))
        result = result.replace(/%f/g, obj.name);
      return result;
    };
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const self = this;
    waterfall([
      //1. Check migrations table existence
      function (cb: (err?: Error, res?: unknown) => void) {
        void self.table('migrations').exists(function (err, exists) {
          if (err) {
            return cb(err);
          }
          return cb(null, exists);
        });
      },
      //2. Create migrations table, if it does not exist
      function (arg: boolean, cb: (err: Error | null, res?: boolean) => void) {
        if (arg) {
          return cb(null, false);
        }
        //create migrations table
        void self.execute('CREATE TABLE migrations("id" INTEGER PRIMARY KEY AUTOINCREMENT, ' +
          '"appliesTo" TEXT NOT NULL, "model" TEXT NULL, "description" TEXT,"version" TEXT NOT NULL)', [], (err) => {
            if (err) {
              return cb(err);
            }
            return cb(null, false);
          });
      },
      //3. Check if migration has already been applied (true=Table version is equal to migration version, false=Table version is older from migration version)
      function (arg: boolean, cb: (err: Error | null, res?: boolean) => void) {
        void self.table(migration.appliesTo).version((err, version) => {
          if (err) {
            return cb(err);
          }
          return cb(null, (version >= migration.version));
        });
      },
      //4a. Check table existence (-1=Migration has already been applied, 0=Table does not exist, 1=Table exists)
      function (arg: boolean, cb: (err: Error | null, res?: number) => void) {
        //migration has already been applied (set migration.updated=true)
        if (arg) {
          migration.updated = true;
          return cb(null, -1);
        }
        else {
          void self.table(migration.appliesTo).exists((err, exists) => {
            if (err) {
              return cb(err);
            }
            return cb(null, exists ? 1 : 0);
          });
        }
      },
      //4. Get table columns
      function (arg: number, cb: (err: Error | null, res?: [number, LocalSqlColumn[]]) => void) {
        //migration has already been applied
        if (arg < 0) {
          return cb(null, [arg, []]);
        }
        void self.table(migration.appliesTo).columns(function (err, columns) {
          if (err) {
            return cb(err);
          }
          return cb(null, [arg, columns]);
        });
      },
      //5. Migrate target table (create or alter)
      function (args: [number, LocalSqlColumn[]], cb: (err: Error | null, res?: number) => void) {
        //migration has already been applied (args[0]=-1)
        if (args[0] < 0) {
          return cb(null, args[0]);
        }
        else if (args[0] === 0) {
          //create table
          const strFields = migration.add.map(function (x) {
            return format('"%f" %t', x);
          }).join(', ');
          const formatter = self.getFormatter();
          const sql = `CREATE TABLE ${formatter.escapeName(migration.appliesTo)} (${strFields})`
          self.execute(sql, null, function (err) {
            if (err) {
              return cb(err);
            }
            return cb(null, 1);
          });
        } else if (args[0] === 1) {
          const expressions: string[] = [];
          const columns = args[1];
          let forceAlter = false;
          let column;
          let newType;
          let oldType;
          //validate operations
          // 1. columns to be removed
          if (Array.isArray(migration.remove)) {
            if (migration.remove.length > 0) {
              for (let i = 0; i < migration.remove.length; i++) {
                const removeColumn = migration.remove[i];
                const colIndex = columns.findIndex((y) => {
                  return y.name === removeColumn.name;
                });
                if (colIndex >= 0) {
                  if (!columns[colIndex].primary) {
                    forceAlter = true;
                  }
                  else {
                    migration.remove.splice(i, 1);
                    i -= 1;
                  }
                }
                else {
                  migration.remove.splice(i, 1);
                  i -= 1;
                }
              }
            }
          }
          //1. columns to be changed
          if (Array.isArray(migration.change)) {
            if (migration.change.length > 0) {
              for (let i = 0; i < migration.change.length; i++) {
                const changeColumn = migration.change[i];
                const column = columns.find((y) => {
                  return y.name === changeColumn.name;
                });
                if (column) {
                  if (!column.primary) {
                    //validate new column type (e.g. TEXT(120,0) NOT NULL)
                    newType = format('%t', changeColumn);
                    oldType = column.type.toUpperCase().concat(column.nullable ? ' NOT NULL' : ' NULL');
                    if ((newType !== oldType)) {
                      //force alter
                      forceAlter = true;
                    }
                  }
                  else {
                    //remove column from change collection (because it's a primary key)
                    migration.change.splice(i, 1);
                    i -= 1;
                  }
                } else {
                  //add column (column was not found in table)
                  migration.add.push(changeColumn);
                  //remove column from change collection
                  migration.change.splice(i, 1);
                  i -= 1;
                }
              }
            }
          }
          if (Array.isArray(migration.add)) {

            // find removed columns
            for (const column of columns) {
              const found = addColumns.find((x) => x.name === column.name);
              if (found == null) {
                forceAlter = true;
              }
            }
            if (forceAlter === false) {
              for (let i = 0; i < migration.add.length; i++) {
                const addColumn = migration.add[i];
                column = columns.find((y) => {
                  return (y.name === addColumn.name);
                });
                if (column) {
                  if (column.primary) {
                    migration.add.splice(i, 1);
                    i -= 1;
                  }
                  else {
                    newType = format('%t', addColumn);
                    oldType = column.type.toUpperCase().concat(column.nullable ? ' NULL' : ' NOT NULL');
                    // trim zero scale for both new and old type
                    // e.g. TEXT(50,0) to TEXT(50)
                    const reTrimScale = /^(NUMERIC|TEXT|INTEGER)\((\d+)(,0)\)/g;
                    if (reTrimScale.test(newType) === true) {
                      // trim
                      newType = newType.replace(reTrimScale, '$1($2)');
                    }
                    if (reTrimScale.test(oldType) === true) {
                      // trim
                      oldType = oldType.replace(reTrimScale, '$1($2)');
                    }
                    if (newType === oldType) {
                      //remove column from add collection
                      migration.add.splice(i, 1);
                      i -= 1;
                    }
                    else {
                      forceAlter = true;
                    }
                  }
                } else {
                  forceAlter = true;
                }
              }
            }
            if (forceAlter) {
              return (async function () {
                // prepare to rename existing table and create a new one
                const renamed = '__' + migration.appliesTo + '_' + new Date().getTime().toString() + '__';
                const formatter = self.getFormatter();
                const renameTable = formatter.escapeName(renamed);
                const table = formatter.escapeName(migration.appliesTo);
                const existingFields = await self.table(migration.appliesTo).columnsAsync();
                // get indexes
                const indexes = await self.indexes(migration.appliesTo).listAsync();
                for (const index of indexes) {
                  await self.indexes(migration.appliesTo).dropAsync(index.name);
                }
                // rename table
                await self.executeAsync(`ALTER TABLE ${table} RENAME TO ${renameTable}`, []);
                // format field collection
                let fields = addColumns.map((x) => {
                  return format('"%f" %t', x);
                }).join(', ');
                let sql = `CREATE TABLE ${table} (${fields})`;
                // create table
                await self.executeAsync(sql, []);
                // get source fields
                const newFields = await self.table(migration.appliesTo).columnsAsync();
                const insertFields = [];
                for (const existingField of existingFields) {
                  const insertField = newFields.find((x) => x.name === existingField.name);
                  if (insertField != null) {
                    insertFields.push(insertField);
                  }
                }
                if (insertFields.length === 0) {
                  throw new Error('Table migration cannot be completed because the collection of fields is empty.');
                }
                fields = insertFields.map((x) => formatter.escapeName(x.name)).join(', ');
                sql = `INSERT INTO ${table}(${fields}) SELECT ${fields} FROM ${renameTable}`;
                // insert data
                await self.executeAsync(sql, []);
              })().then(() => {
                return cb(null, 1);
              }).catch((error) => {
                return cb(error);
              });
            }
            else {
              const formatter = self.getFormatter();
              migration.add.forEach(function (x) {
                //search for columns
                const strTable = formatter.escapeName(migration.appliesTo);
                const strColumn = formatter.escapeName(x.name);
                const strType = self.formatType(x);
                const expression = `ALTER TABLE ${strTable} ADD COLUMN ${strColumn} ${strType}`;
                expressions.push(expression);
              });
            }
          }
          if (expressions.length > 0) {
            eachSeries(expressions, function (expr, cb) {
              self.execute(expr, [], function (err) {
                cb(err);
              });
            }, function (err) {
              if (err) {
                return cb(err);
              }
              return cb(null, 1);
            });
          }
          else {
            return cb(null, 2);
          }
        }
        else {
          return cb(new Error('Invalid table status.'));
        }
      },
      //Apply data model indexes
      function (arg: number, cb: (err: Error | null, res?: number) => void) {
        if (arg <= 0) {
          return cb(null, arg);
        }
        if (migration.indexes) {
          const tableIndexes = self.indexes(migration.appliesTo);
          //enumerate migration constraints
          eachSeries(migration.indexes, function (index, indexCallback) {
            tableIndexes.create(index.name, index.columns, indexCallback);
          }, function (err) {
            //throw error
            if (err) {
              return cb(err);
            }
            //or return success flag
            return cb(null, 1);
          });
        }
        else {
          //do nothing and exit
          return cb(null, 1);
        }
      },
      function (arg: number, cb: (err: Error | null, res?: number) => void) {
        if (arg > 0) {
          //log migration to database
          self.execute('INSERT INTO migrations("appliesTo", "model", "version", "description") VALUES (?,?,?,?)', [migration.appliesTo,
          migration.model,
          migration.version,
          migration.description], function (err) {
            if (err) {
              return cb(err);
            }
            cb(null, 1);
          });
        }
        else {
          migration.updated = true;
          cb(null, arg);
        }
      }
    ], function (err) {
      callback(err);
    });
  }

  migrateAsync(obj: LocalSqlTableUpgrade): Promise<void> {
    return new Promise((resolve, reject) => {
      this.migrate(obj, (err?: Error) => {
        if (err) {
          return reject(err);
        }
        return resolve(void 0);
      });
    });
  }

  selectIdentity(entity: string, attribute: string, callback: (err?: Error, value?: number) => void): void {
    const migration: LocalSqlTableUpgrade = {
      appliesTo: 'increment_id',
      model: 'increments',
      description: 'Increments migration (version 1.0)',
      version: '1.0',
      add: [
        { name: 'id', type: 'Counter', primary: true },
        { name: 'entity', type: 'Text', size: 120 },
        { name: 'attribute', type: 'Text', size: 120 },
        { name: 'value', type: 'Integer' }
      ]
    };
    //ensure increments entity
    this.migrate(migration, (err?: Error) => {
      //throw error if any
      if (err) {
        return callback(err);
      }
      void this.execute('SELECT * FROM increment_id WHERE entity=? AND attribute=?', [entity, attribute], (err?: Error, result?: Record<string, unknown>[]) => {
        if (err) {
          return callback(err);
        }
        if (result.length === 0) {
          //get max value by querying the given entity
          const q = new QueryExpression().from(entity).select([new QueryField().max(attribute)]);
          void this.execute(q, null, (err?: Error, result?: Record<string, unknown>[]) => {
            if (err) {
              return callback(err);
            }
            let value = 1;
            if (result.length > 0) {
              value = (parseInt(result[0][attribute] as string) || 0) + 1;
            }
            void this.execute('INSERT INTO increment_id(entity, attribute, value) VALUES (?,?,?)', [entity, attribute, value], (err) => {
              // throw error if any
              if (err) {
                return callback(err);
              }
              // return new increment value
              return callback(null, value);
            });
          });
        } else {
          //get new increment value
          const value = parseInt(result[0].value as string) + 1;
          void this.execute('UPDATE increment_id SET value=? WHERE id=?', [value, result[0].id], (err) => {
            // throw error if any
            if (err) {
              return callback(err);
            }
            // return new increment value
            return callback(null, value);
          });
        }
      });
    });
  }

  selectIdentityAsync(entity: string, attribute: string) {
    return new Promise((resolve, reject) => {
      return this.selectIdentity(entity, attribute, (err, value) => {
        if (err) {
          return reject(err);
        }
        return resolve(value);
      });
    });
  }

  table(name: string): LocalSqlTable {
    // return new LocalSqlDataTable(this, name);
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const self = this;
    return {
      exists: function (callback: (err?: Error, value?: boolean) => void): void {
        void self.execute('SELECT COUNT(*) count FROM sqlite_master WHERE name=? AND type=\'table\';', [name], (err, results?: { count: number }[]) => {
          if (err) {
            return callback(err);
          }
          const [result] = results || [];
          return callback(null, (result.count > 0));
        });
      },
      existsAsync: function (): Promise<boolean> {
        return new Promise((resolve, reject) => {
          this.exists((err?: Error, value?: boolean) => {
            if (err) {
              return reject(err);
            }
            return resolve(value);
          });
        });
      },
      version(callback: (err?: Error, value?: string) => void): void {
        void self.execute('SELECT MAX(version) AS version FROM migrations WHERE appliesTo=?', [this.name], (err?: Error, results?: { version: string }[]) => {
          if (err) {
            return callback(err);
          }
          if (results.length === 0) {
            callback(null, '0.0');
          }
          return callback(null, results[0].version || '0.0');
        });
      },
      versionAsync: function () {
        return new Promise((resolve, reject) => {
          this.version((err?: Error, value?: string) => {
            if (err) {
              return reject(err);
            }
            return resolve(value);
          });
        });
      },
      columns: function (callback: (err?: Error, results?: LocalSqlColumn[]) => void): void {
        void self.execute('PRAGMA table_info(?)', [name], (err?: Error, columns?: { name: string, type: string, cid: number, notnull: number, pk: number }[]) => {
          if (err) {
            return callback(err);
          }
          const results = columns.map((item) => {
            const col: LocalSqlColumn = { name: item.name, ordinal: item.cid, type: item.type, nullable: (item.notnull ? false : true), primary: (item.pk === 1) };
            const matches = /(\w+)\((\d+),(\d+)\)/.exec(item.type);
            if (matches) {
              //extract max length attribute (e.g. integer(2,0) etc)
              if (parseInt(matches[2]) > 0) {
                col.size = parseInt(matches[2]);
              }
              //extract scale attribute from field (e.g. integer(2,0) etc)
              if (parseInt(matches[3]) > 0) {
                col.scale = parseInt(matches[3]);
              }
            }
            return col;
          });
          return callback(null, results);
        });
      },
      columnsAsync: function (): Promise<LocalSqlColumn[]> {
        return new Promise((resolve, reject) => {
          this.columns((err?: Error, res?: LocalSqlColumn[]) => {
            if (err) {
              return reject(err);
            }
            return resolve(res);
          });
        });
      },
      create: function (fields: LocalSqlField[], callback: (err?: Error) => void): void {
        //create table
        const containerWithFormat = self as unknown as { format: (format: string, arg: LocalSqlField) => string };
        const strFields = fields.map((field) => {
          return containerWithFormat.format('"%f" %t', field);
        }).join(', ');
        const sql = `CREATE TABLE "${name}" (${strFields})`;
        void self.execute(sql, null, (err?: Error) => {
          if (err) {
            return callback(err);
          }
          return callback();
        });
      },
      createAsync: function (fields: LocalSqlField[]) {
        return new Promise((resolve, reject) => {
          this.create(fields, (err?: Error) => {
            if (err) {
              return reject(err);
            }
            return resolve(void 0);
          });
        });
      },
      add: function (fields: LocalSqlField[], callback: (err?: Error) => void): void {
        if (Array.isArray(fields) === false) {
          //invalid argument exception
          return callback(new Error('Invalid argument type. Expected Array.'));
        }
        if (fields.length === 0) {
          // do nothing
          return callback();
        }
        // generate SQL statement
        const formatter = self.getFormatter();
        const escapedTable = formatter.escapeName(name);
        const containerWithFormatType = self as unknown as { formatType: (arg: LocalSqlField) => string };
        const sql = fields.map((field) => {
          const escapedField = formatter.escapeName(field.name);
          return `ALTER TABLE ${escapedTable} ADD COLUMN ${escapedField} ${containerWithFormatType.formatType(field)}`;
        }).join(';');
        self.execute(sql, [], function (err?: Error) {
          callback(err);
        });
      },
      addAsync: function (fields: LocalSqlField[]): Promise<void> {
        return new Promise((resolve, reject) => {
          this.add(fields, (err?: Error) => {
            if (err) {
              return reject(err);
            }
            return resolve(void 0);
          });
        });
      },
      change: function (fields: LocalSqlField[], callback: (err?: Error) => void): void {
        return callback(new Error('Full table migration is not yet implemented.'));
      },
      changeAsync: function (fields: LocalSqlField[]) {
        return new Promise((resolve, reject) => {
          this.change(fields, (err?: Error) => {
            if (err) {
              return reject(err);
            }
            return resolve(void 0);
          });
        });
      }
    } as LocalSqlTable;
  }

  view(name: string): LocalSqlView {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const self = this;
    return {
      exists: function (callback: (err?: Error, value?: boolean) => void) {
        void self.execute('SELECT COUNT(*) count FROM sqlite_master WHERE name=? AND type=\'view\';', [name], (err, result: { count: number }[]) => {
          if (err) {
            return callback(err);
          }
          return callback(null, (result[0].count > 0));
        });
      },
      existsAsync: function (): Promise<boolean> {
        return new Promise((resolve, reject) => {
          this.exists((err?: Error, value?: boolean) => {
            if (err) {
              return reject(err);
            }
            return resolve(value);
          });
        });
      },

      drop: function (callback: (err?: Error) => void) {
        void self.open((err) => {
          if (err) {
            return callback(err);
          }
          const sql = `DROP VIEW IF EXISTS ${name}`;
          self.execute(sql, [], (err) => {
            if (err) {
              return callback(err);
            }
            callback();
          });
        });
      },
      dropAsync: function (): Promise<void> {
        return new Promise((resolve, reject) => {
          void this.drop((err?: Error) => {
            if (err) {
              return reject(err);
            }
            return resolve();
          });
        });
      },
      create: function (q: QueryExpression | string, callback: (err?: Error) => void) {
        // eslint-disable-next-line @typescript-eslint/no-this-alias
        const thisArg = this;
        void self.executeInTransaction((transactionCallback) => {
          thisArg.drop((err?: Error) => {
            if (err) {
              return transactionCallback(err);
            }
            try {
              const formatter = self.getFormatter();
              let sql = `CREATE VIEW ${formatter.escapeName(name)} AS`;
              sql += ' ';
              sql += formatter.format(q);
              self.execute(sql, [], transactionCallback);
            }
            catch (e) {
              transactionCallback(e);
            }
          });
        }, (err) => {
          callback(err);
        });
      },
      createAsync: function (q: QueryExpression | string): Promise<void> {
        return new Promise((resolve, reject) => {
          this.create(q, (err?: Error) => {
            if (err) {
              return reject(err);
            }
            return resolve();
          });
        });
      }
    } as LocalSqlView;
  }

  @before(({ target, args }, callback) => {
    const [query, params] = args;
    void target.executing.emit({
      target,
      query,
      params
    }).then(() => {
      return callback(null);
    }).catch((err: Error) => {
      return callback(err);
    });
  })
  @after(({ target, args, result: results }, callback) => {
    const [query, params] = args;
    const event = {
      target,
      query,
      params,
      results
    };
    void target.executed.emit(event).then(() => {
      return callback(null, {
        value: results
      });
    }).catch((err?: Error) => {
      return callback(err);
    });
  })
  // eslint-disable-next-line @typescript-eslint/consistent-indexed-object-style
  execute(query: string | QueryExpression | unknown, values: unknown[], callback: (err?: Error, result?: unknown | { [k: string]: unknown }) => void): void {

    let sql: string;
    try {
      if (typeof query === 'string') {
        //get raw sql statement
        sql = query;
      }
      else {
        //format query expression or any object that may act as query expression
        const formatter = this.getFormatter();
        sql = formatter.format(query);
      }
      if (typeof sql !== 'string') {
        return callback(new Error('The executing command is of the wrong type or empty.'));
      }
      this.open((err?: Error) => {
        if (err) {
          return callback(err);
        }
        else {
          //prepare statement - the traditional way
          const prepared = this.prepare(sql, values);
          //execute raw command
          ((executeSql: string, executeCallback: (err?: Error, result?: unknown) => void) => {
            try {
              const results = this.rawConnection.exec(executeSql);
              const [result] = results;
              if (result && result.columns && result.values) {
                const keys = result.columns;
                if (keys.length === 0) {
                  return executeCallback(null, []);
                }
                const values = result.values.map((item) => {
                  return keys.reduce(function (acc, key, index) {
                    acc[key] = item[index];
                    return acc;
                  }, {} as Record<string, unknown>);
                });
                return executeCallback(null, values);
              }
              return executeCallback(null, []);
            } catch (executeError) {
              return executeCallback(executeError);
            }
          })(prepared, (err?: Error, result?: unknown) => {
            const callbackWithRetry = callback as { retry?: number };
            if (err) {
              const errWithCode = err as { code?: string };
              if (errWithCode.code === 'SQLITE_BUSY') {
                const shouldRetry = typeof this.options.retry === 'number' && this.options.retry > 0;
                if (shouldRetry === false) {
                  return callback(err);
                }
                const retry = this.options.retry;
                let retryInterval = 1000;
                if (typeof this.options.retryInterval === 'number' && this.options.retryInterval > 0) {
                  retryInterval = this.options.retryInterval;
                }
                // validate retry option
                if (Object.prototype.hasOwnProperty.call(callback, 'retry') === false) {
                  Object.defineProperty(callback, 'retry', {
                    configurable: true,
                    enumerable: false,
                    value: 0,
                    writable: true
                  });
                }
                if (typeof callbackWithRetry.retry === 'number' && callbackWithRetry.retry >= (retry * retryInterval)) {
                  delete callbackWithRetry.retry;
                  return callback(err);
                }
                // retry
                callbackWithRetry.retry += retryInterval;
                return setTimeout(() => {
                  this.execute(query, values, callback);
                }, callbackWithRetry.retry);
              }
              // log sql
              if (Object.prototype.hasOwnProperty.call(callback, 'retry')) {
                delete callbackWithRetry.retry;
              }
              callback(err);
            }
            else {
              if (Object.prototype.hasOwnProperty.call(callback, 'retry')) {
                delete callbackWithRetry.retry;
              }
              if (result) {
                if (typeof result === 'object') {
                  if (Array.isArray(result)) {
                    if (result.length > 0) {
                      const keys = Object.keys(result[0]);
                      result.forEach(function (x) {
                        keys.forEach(function (y) {
                          const value = x[y];
                          if (typeof value === 'string' && SqlDateRegEx.test(value)) {
                            x[y] = new Date(value);
                          }
                        });
                      });
                    }
                  } else {
                    // eslint-disable-next-line @typescript-eslint/consistent-indexed-object-style
                    const resultAsObject = result as { [k: string]: unknown };
                    Object.keys(resultAsObject).forEach((key: string) => {
                      const value = resultAsObject[key];
                      if (typeof value === 'string' && SqlDateRegEx.test(value)) {
                        resultAsObject[key] = new Date(value);
                      }
                    });
                  }
                }
                return callback(null, result);
              } else {
                return callback();
              }
            }
          });
        }
      });
    }
    catch (e) {
      return callback(e);
    }
  }

  executeAsync<T>(query: string | QueryExpression | unknown, values?: unknown[]): Promise<T[]> {
    return new Promise((resolve, reject) => {
      void this.execute(query, values, (err?: Error, res?: T[]) => {
        if (err) {
          return reject(err);
        }
        return resolve(res);
      });
    });
  }

  lastIdentity(callback: (err?: Error, value?: { insertId: number }) => void) {
    void this.open((err) => {
      if (err) {
        return callback(err);
      }
      void this.execute('SELECT last_insert_rowid() as lastval', [], (err: Error, lastval: { lastval?: number }[]) => {
        if (err) {
          return callback(null, { insertId: null });
        } else {
          lastval = lastval || [];
          if (lastval.length > 0)
            return callback(null, { insertId: lastval[0].lastval });
          else
            return callback(null, { insertId: null });
        }
      });
    });
  }

  lastIdentityAsync() {
    return new Promise((resolve, reject) => {
      return this.lastIdentity((err, value) => {
        if (err) {
          return reject(err);
        }
        return resolve(value);
      });
    });
  }

  indexes(table: string): LocalSqlIndexCollection {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const self = this;
    const formatter = this.getFormatter();
    return {
      list: function (callback: (err?: Error, results?: { name: string, columns: string[] }[]) => void) {
        // eslint-disable-next-line @typescript-eslint/no-this-alias
        const thisArg: { _indexes?: { name: string, columns: string[] }[] } = this;
        if (Object.prototype.hasOwnProperty.call(thisArg, '_indexes')) {
          return callback(null, thisArg._indexes);
        }
        self.execute(`PRAGMA INDEX_LIST(${formatter.escape(table)})`, [], (err, result: { origin: string, name: string }[]) => {
          if (err) {
            return callback(err);
          }
          const indexes: { name: string, columns: string[] }[] = result.filter(function (x) {
            return x.origin === 'c';
          }).map(function (x) {
            return {
              name: x.name,
              columns: [] as string[]
            };
          });
          eachSeries(indexes, function (index, cb) {
            self.execute(`PRAGMA INDEX_INFO(${formatter.escapeName(index.name)})`, [], (err, columns: { name: string }[]) => {
              if (err) {
                return cb(err);
              }
              index.columns = columns.map(function (x) {
                return x.name;
              });
              return cb();
            });
          }, function (err) {
            if (err) {
              return callback(err);
            }
            thisArg._indexes = indexes;
            return callback(null, indexes);
          });
        });
      },
      listAsync: function (): Promise<{ name: string, columns: string[] }[]> {
        return new Promise((resolve, reject) => {
          this.list((err?: Error, results?: { name: string, columns: string[] }[]) => {
            if (err) {
              return reject(err);
            }
            return resolve(results);
          });
        });
      },

      create: function (name: string, columns: string[], callback: (err?: Error) => void) {
        // eslint-disable-next-line @typescript-eslint/no-this-alias
        const thisArg = this;
        void thisArg.list((err: Error, indexes: { name: string, columns: string[] }[]) => {
          if (err) {
            return callback(err);
          }
          const findIndex = indexes.find((x: { name: string, columns: string[] }) => { return x.name === name; });
          //format create index SQL statement
          const strColumns = columns.map(function (x) {
            return formatter.escapeName(x);
          }).join(',');
          const strTable = formatter.escapeName(table);
          const strName = formatter.escapeName(name);
          const sqlCreateIndex = `CREATE INDEX ${strName} ON ${strTable}(${strColumns})`;
          if (findIndex == null) {
            return self.execute(sqlCreateIndex, [], (err) => {
              return callback(err)
            });
          } else {
            let nCols = columns.length;
            //enumerate existing columns
            findIndex.columns.forEach((x) => {
              if (columns.indexOf(x) >= 0) {
                //column exists in index
                nCols -= 1;
              }
            });
            if (nCols > 0) {
              //drop index
              return thisArg.drop(name, (err?: Error) => {
                if (err) {
                  return callback(err);
                }
                //and create it
                self.execute(sqlCreateIndex, [], callback);
              });
            }
            return callback();
          }
        });
      },
      createAsync: function (name: string, columns: string[]): Promise<void> {
        return new Promise((resolve, reject) => {
          this.create(name, columns, (err?: Error) => {
            if (err) {
              return reject(err);
            }
            return resolve(void 0);
          });
        });
      },
      drop: function (name: string, callback: (err?: Error) => void) {
        if (typeof name !== 'string') {
          return callback(new Error('Name must be a valid string.'));
        }
        void self.execute(`PRAGMA INDEX_LIST(${formatter.escape(table)})`, [], function (err, result: { name: string }[]) {
          if (err) {
            return callback(err);
          }
          const exists = typeof result.find(function (x) { return x.name === name; }) !== 'undefined';
          if (!exists) {
            return callback();
          }
          const formatter = self.getFormatter();
          self.execute(`DROP INDEX ${formatter.escapeName(name)}`, [], callback);
        });
      },
      dropAsync: function (name: string): Promise<void> {
        return new Promise((resolve, reject) => {
          this.drop(name, (err?: Error) => {
            if (err) {
              return reject(err);
            }
            return resolve(void 0);
          });
        });
      }
    } as LocalSqlIndexCollection;
  }

  getFormatter() {
    return new LocalSqlFormatter();
  }

}

export {
  LocalSqlTableUpgrade,
  LocalSqlColumn,
  LocalSqlField,
  LocalSqlTable,
  LocalSqlIndex,
  LocalSqlIndexCollection,
  LocalSqlView,
  LocalSqlAdapter
}
