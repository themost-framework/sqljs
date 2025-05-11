import '@testing-library/jest-dom/jest-globals';
// eslint-disable-next-line @typescript-eslint/no-unused-vars
import {render, screen} from '@testing-library/react';
import { expect } from '@jest/globals';
import { LocalSqlAdapter } from "@themost/sql.js";
import { fetch } from 'cross-fetch';
import { QueryExpression } from '@themost/query';

describe('LocalSqlAdapter', () => {
    it('should create instance', () => {
        expect(new LocalSqlAdapter()).toBeInstanceOf(LocalSqlAdapter);
    });

    it('should load database', async () => {
        const response = await fetch('http://localhost:3000/assets/db/local.db');
        const buffer = await response.arrayBuffer();
        const db = new LocalSqlAdapter({
            buffer: new Uint8Array(buffer)
        });
        expect(db).toBeInstanceOf(LocalSqlAdapter);
        const Users = 'UserData';
        const exists = await db.view(Users).existsAsync();
        expect(exists).toBe(true);
        let [user] = await db.executeAsync<{ id: number, name: string, image?: string, description?: string }>(
            new QueryExpression().select('id', 'name', 'description', 'image').from(Users).where('name').equal('alexis.rees@example.com')
        );
        expect(user).toBeDefined();
        user.image = 'https://randomuser.me/api/portraits/med/men/52.jpg';
        await db.executeAsync(
            new QueryExpression().update('ThingBase').set({
                image: user.image
            }).where('id').equal(user.id)
        );
        [user] = await db.executeAsync<{ id: number, name: string, image?: string, description?: string }>(
            new QueryExpression().select('id', 'name', 'description', 'image').from(Users).where('name').equal('alexis.rees@example.com')
        );
        expect(user).toBeDefined();
        expect(user.image).toBe('https://randomuser.me/api/portraits/med/men/52.jpg');
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
