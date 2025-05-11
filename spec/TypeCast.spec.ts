import '@testing-library/jest-dom/jest-globals';
// eslint-disable-next-line @typescript-eslint/no-unused-vars
import {render, screen} from '@testing-library/react';
import { expect } from '@jest/globals';
import { LocalSqlAdapter } from "@themost/sql.js";
import { fetch } from 'cross-fetch';
import { QueryExpression, QueryField, QueryEntity } from '@themost/query';

describe('Type Casting', () => {

    let db: LocalSqlAdapter;
    beforeAll(async () => {
        const response = await fetch('http://localhost:3000/assets/db/local.db');
        const buffer = await response.arrayBuffer();
        db = new LocalSqlAdapter({
            buffer: new Uint8Array(buffer)
        });
    });
    
    it('should use uuid()', async () => {
        const query = new QueryExpression().select(new QueryField({
            id: {
                $uuid: []
            }
        })).from(new QueryEntity('t0'));
        query.$fixed = true;
        const [item] = await db.executeAsync<{ id: string }>(query, []);
        expect(item).toBeTruthy();
        expect(item.id).toBeTruthy();
    });

    it('should use getDate()', async () => {
        const query = new QueryExpression().select(new QueryField({
            currentDate: {
                $getDate: [
                    'date'
                ]
            }
        })).from(new QueryEntity('t0'));
        query.$fixed = true;
        const [item] = await db.executeAsync<{ currentDate: string }>(query, []);
        expect(item).toBeTruthy();
        expect(typeof item.currentDate === 'string').toBeTruthy()
    });

    it('should use getDate() for datetime', async () => {
        const query = new QueryExpression().select(new QueryField({
            currentDateTime: {
                $getDate: [
                    'datetime'
                ]
            }
        })).from(new QueryEntity('t0'));
        query.$fixed = true;
        const [item] = await db.executeAsync<{ currentDateTime: Date }>(query, []);
        expect(item).toBeTruthy();
        expect(item.currentDateTime instanceof Date).toBeTruthy()
    });

    it('should use getDate() for timestamp', async () => {
        const query = new QueryExpression().select(new QueryField({
            currentDateTime: {
                $getDate: [
                    'timestamp'
                ]
            }
        })).from(new QueryEntity('t0'));
        query.$fixed = true;
        const [item] = await db.executeAsync<{ currentDateTime: Date }>(query, []);
        expect(item).toBeTruthy();
        expect(item.currentDateTime instanceof Date).toBeTruthy()
    });

    it('should use toGuid()', async () => {
        const query = new QueryExpression().select(new QueryField({
            id: {
                $toGuid: [
                    'Hello'
                ]
            }
        })).from(new QueryEntity('t0'));
        query.$fixed = true;
        const [item] = await db.executeAsync<{ id: string }>(query, []);
        expect(item).toBeTruthy();
        expect(item.id.toLowerCase()).toEqual('8b1a9953-c461-1296-a827-abf8c47804d7');
    });
});