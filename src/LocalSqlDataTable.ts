import { LocalSqlAdapterBase } from "./LocalSqlAdapterBase";

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
    oneToMany?: boolean;
}

class LocalSqlDataTable {
    constructor(private container: LocalSqlAdapterBase, private name: string) {
        this.container = container;
        this.name = name;
    }

    exists(callback: (err?: Error, value?: boolean) => void): void {
        void this.container.execute('SELECT COUNT(*) count FROM sqlite_master WHERE name=? AND type=\'table\';', [this.name], (err, results?: { count: number }[]) => {
            if (err) {
                return callback(err);
            }
            const [result] = results || [];
            return callback(null, (result.count > 0));
        });
    }

    existsAsync() {
        return new Promise((resolve, reject) => {
            this.exists((err, value) => {
                if (err) {
                    return reject(err);
                }
                return resolve(value);
            });
        });
    }

    version(callback: (err?: Error, value?: string) => void): void {
        void this.container.execute('SELECT MAX(version) AS version FROM migrations WHERE appliesTo=?', [this.name], (err?: Error, results?: {version: string}[]) => {
            if (err) {
                return callback(err);
            }
            if (results.length === 0) {
                callback(null, '0.0');
            }
            return callback(null, results[0].version || '0.0');
        });
    }

    versionAsync() {
        return new Promise((resolve, reject) => {
            this.version((err, value) => {
                if (err) {
                    return reject(err);
                }
                return resolve(value);
            });
        });
    }

    columns(callback: (err?: Error, results?: LocalSqlColumn[]) => void): void {
        void this.container.execute('PRAGMA table_info(?)', [this.name], (err?: Error, columns?: { name: string, type: string, cid: number, notnull: number, pk: number }[]) => {
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
    }

    columnsAsync(): Promise<LocalSqlColumn[]> {
        return new Promise((resolve, reject) => {
            this.columns((err, res) => {
                if (err) {
                    return reject(err);
                }
                return resolve(res);
            });
        });
    }

    create(fields: LocalSqlField[], callback: (err?: Error) => void): void {
        //create table
        const containerWithFormat = this.container as unknown as { format: (format: string, arg: LocalSqlField) => string };
        const strFields = fields.filter((field) => {
            return !field.oneToMany;
        }).map(function (field) {
            return containerWithFormat.format('"%f" %t', field);
        }).join(', ');
        const sql = `CREATE TABLE "${this.name}" (${strFields})`;
        void this.container.execute(sql, null, (err) => {
            if (err) {
                return callback(err);
            }
            return callback();
        });
    }

    createAsync(fields: LocalSqlField[]) {
        return new Promise((resolve, reject) => {
            this.create(fields, (err?: Error) => {
                if (err) {
                    return reject(err);
                }
                return resolve(void 0);
            });
        });
    }

    add(fields: LocalSqlField[], callback: (err?: Error) => void): void {
        if (Array.isArray(fields) === false) {
            //invalid argument exception
            return callback(new Error('Invalid argument type. Expected Array.'));
        }
        if (fields.length === 0) {
            // do nothing
            return callback();
        }
        // generate SQL statement
        const formatter = this.container.getFormatter();
        const escapedTable = formatter.escapeName(this.name);
        const containerWithFormatType = this.container as unknown as { formatType: (arg: LocalSqlField) => string };
        const sql = fields.map((field) => {
            const escapedField = formatter.escapeName(field.name);
            return `ALTER TABLE ${escapedTable} ADD COLUMN ${escapedField} ${containerWithFormatType.formatType(field)}`;
        }).join(';');
        this.container.execute(sql, [], function (err) {
            callback(err);
        });
    }

    addAsync(fields: LocalSqlField[]) {
        return new Promise((resolve, reject) => {
            this.add(fields, (err?: Error) => {
                if (err) {
                    return reject(err);
                }
                return resolve(void 0);
            });
        });
    }

    change(fields: LocalSqlField[], callback: (err?: Error) => void): void {
        return callback(new Error('Full table migration is not yet implemented.'));
    }

    changeAsync(fields: LocalSqlField[]) {
        return new Promise((resolve, reject) => {
            this.change(fields, (err?: Error) => {
                if (err) {
                    return reject(err);
                }
                return resolve(void 0);
            });
        });
    }
}

export {
    LocalSqlField,
    LocalSqlColumn,
    LocalSqlDataTable
}