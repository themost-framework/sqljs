import '@testing-library/jest-dom/jest-globals';
import { expect } from '@jest/globals';
import {LocalSqlAdapter} from "@themost/sqljs";

describe('sql.js', () => {
    it('should create instance', () => {
        expect(new LocalSqlAdapter()).toBeInstanceOf(LocalSqlAdapter);
    });
});
