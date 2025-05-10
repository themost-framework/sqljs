import { QueryExpression, SqlFormatter } from "@themost/query";

export interface LocalSqlAdapterBase {
    open(callback:(err?:Error) => void): void;
    openAsync(): Promise<void>;
    close(callback:(err?:Error) => void): void;
    closeAsync(): Promise<void>;
    execute(query: string | QueryExpression | unknown, values: unknown[], callback: (err?: Error, result?: unknown | Record<string, unknown>) => void): void;
    executeAsync(query: string | QueryExpression | unknown, values: unknown[]): Promise<unknown | Record<string, unknown>>;
    executeInTransaction(func: (callback: (err?: Error) => void) => void, callback: (err?: Error) => void): void;
    executeInTransactionAsync(func: () => Promise<void>): Promise<void>;
    getFormatter(): SqlFormatter;
}