import '@testing-library/jest-dom/jest-globals';
import { expect } from '@jest/globals';
import { LocalSqlAdapter } from "@themost/sqljs";

describe('LocalSqlAdapter', () => {
    it('should create instance', () => {
        expect(new LocalSqlAdapter()).toBeInstanceOf(LocalSqlAdapter);
    });

    it('should create table', async () => {
        const db = new LocalSqlAdapter();
        let exists = await db.table('test').existsAsync();
        expect(exists).toBe(false);
        await db.table('test').createAsync([
            { name: 'id', type: 'Counter', primary: true },
            { name: 'name', type: 'Text' },
            { name: 'age', type: 'Integer' }
        ]);
        exists = await db.table('test').existsAsync();
        expect(exists).toBe(true);
        const db2 = new LocalSqlAdapter();
        exists = await db2.table('test').existsAsync();
        expect(exists).toBe(true);
    });
});
