import {QueryField, SqlFormatter} from '@themost/query';
import isPlainObject from 'lodash/isPlainObject';
import isObjectLike from 'lodash/isObjectLike';
import isNative from 'lodash/isNative';

const REGEXP_SINGLE_QUOTE=/\\'/g;
const SINGLE_QUOTE_ESCAPE ='\'\'';
const DOUBLE_QUOTE_ESCAPE = '"';
const REGEXP_SLASH=/\\\\/g;
const SLASH_ESCAPE = '\\';


const objectToString = Function.prototype.toString.call(Object);

function isObjectDeep(value: unknown) {
    // check if it is a plain object
    let result = isPlainObject(value);
    if (result) {
        return result;
    }
    // check if it's object
    if (isObjectLike(value) === false) {
        return false;
    }
    // get prototype
    let proto = Object.getPrototypeOf(value);
    // if prototype exists, try to validate prototype recursively
    while(proto != null) {
        // get constructor
        const Ctor = Object.prototype.hasOwnProperty.call(proto, 'constructor')
            && proto.constructor;
        // check if constructor is native object constructor
        result = (typeof Ctor == 'function') && (Ctor instanceof Ctor)
            && Function.prototype.toString.call(Ctor) === objectToString;
        // if constructor is not object constructor and belongs to a native class
        if (result === false && isNative(Ctor) === true) {
            // return false
            return false;
        }
        // otherwise. get parent prototype and continue
        proto = Object.getPrototypeOf(proto);
    }
    // finally, return result
    return result;
}


// noinspection JSUnusedLocalSymbols
function timezone(): string {
    const offset = new Date().getTimezoneOffset();
    return (offset <= 0 ? '+' : '-') + zeroPad(-Math.floor(offset / 60), 2) + ':' + zeroPad(offset % 60, 2);
}

function zeroPad(number: number, length?: number) {
    number = number || 0;
    let res = number.toString();
    while (res.length < length) {
        res = '0' + res;
    }
    return res;
}

class LocalSqlFormatter extends SqlFormatter {
    static readonly NAME_FORMAT = '"$1"';
    constructor() {
        super();
        this.settings.nameFormat = LocalSqlFormatter.NAME_FORMAT;
        this.settings.forceAlias = true;
    }

    /**
     * Escapes an object or a value and returns the equivalent sql value.
     * @param {*} value - A value that is going to be escaped for SQL statements
     * @param {boolean=} unquoted - An optional value that indicates whether the resulted string will be quoted or not.
     * returns {string} - The equivalent SQL string value
     */
    escape(value: unknown, unquoted?: boolean): string {
        if (typeof value === 'boolean') {
            return value ? '1' : '0';
        }
        if (value instanceof Date) {
            return this.escapeDate(value);
        }
        // serialize array of objects as json array
        if (Array.isArray(value)) {
            // find first non-object value
            const index = value.filter((x) => {
                return x != null;
            }).findIndex((x) => {
                return isObjectDeep(x) === false;
            });
            // if all values are objects
            if (index === -1) {
                return this.escape(JSON.stringify(value)); // return as json array
            }
        }
        let res = super.escape.bind(this)(value, unquoted);
        if (typeof value === 'string') {
            if (REGEXP_SINGLE_QUOTE.test(res))
                //escape single quote (that is already escaped)
                res = res.replace(/\\'/g, SINGLE_QUOTE_ESCAPE);
            //escape double quote (that is already escaped)
            res = res.replace(/\\"/g, DOUBLE_QUOTE_ESCAPE);
            if (REGEXP_SLASH.test(res))
                //escape slash (that is already escaped)
                res = res.replace(/\\\\/g, SLASH_ESCAPE);
        }
        return res;
    }

    escapeDate(val: Date): string {
        const year = val.getFullYear();
        const month = zeroPad(val.getMonth() + 1, 2);
        const day = zeroPad(val.getDate(), 2);
        const hour = zeroPad(val.getHours(), 2);
        const minute = zeroPad(val.getMinutes(), 2);
        const second = zeroPad(val.getSeconds(), 2);
        const millisecond = zeroPad(val.getMilliseconds(), 3);
        //format timezone
        const offset = val.getTimezoneOffset(), timezone = (offset <= 0 ? '+' : '-') + zeroPad(-Math.floor(offset / 60), 2) + ':' + zeroPad(offset % 60, 2);
        return '\'' + year + '-' + month + '-' + day + ' ' + hour + ':' + minute + ':' + second + '.' + millisecond + timezone + '\'';
    }

    $indexof(p0: unknown, p1: unknown) {
        return this.$indexOf(p0, p1);
    }

    $indexOf(p0: unknown, p1: unknown) {
        return `(INSTR(${this.escape(p0)},${this.escape(p1)})-1)`;
    }

    $text(p0: unknown, p1: unknown): string {
        return `(INSTR(${this.escape(p0)},${this.escape(p1)})-1)>=0`;
    }

    $regex(p0: unknown, p1: unknown): string {
        //escape expression
        let s1 = this.escape(p1, true);
        //implement starts with equivalent for LIKE T-SQL
        if (/^\^/.test(s1)) {
            s1 = s1.replace(/^\^/, '');
        }
        else {
            s1 = '%' + s1;
        }
        //implement ends with equivalent for LIKE T-SQL
        if (/\$$/.test(s1)) {
            s1 = s1.replace(/\$$/, '');
        }
        else {
            s1 += '%';
        }
        return `LIKE(${this.escape(s1)},${this.escape(p0)}) >= 1`;
    }

    $concat(...arg: unknown[]): string {
        const args = arg;
        if (args.length < 2) {
            throw new Error('Concat method expects two or more arguments');
        }
        let result = '(';
        result += Array.from(args).map((arg) => {
            return `IFNULL(${this.escape(arg)},'')`
        }).join(' || ');
        result += ')';
        return result;
    }

    $substring(p0: unknown, pos: unknown, length?: number): string {
        if (length) {
            return `SUBSTR(${this.escape(p0)},${this.escape(pos)} + 1,${this.escape(length)})`
        } else {
            return `SUBSTR(${this.escape(p0)},${this.escape(pos)} + 1)`;
        }
    }

    $substr(p0: unknown, pos: unknown, length?: number): string {
        return this.$substring(p0, pos, length);
    }

    $length(p0: unknown): string {
        return `LENGTH(${this.escape(p0)})`;
    }

    $ceiling(p0: unknown): string {
        return `CEIL(${this.escape(p0)})`;
    }

    $startswith(p0: unknown, p1: unknown): string {
        return this.$startsWith(p0, p1);
    }

    $startsWith(p0: unknown, p1: unknown): string {
        //validate params
        if (p0 == null || p1 == null)
            return '';
        return 'LIKE(\'' + this.escape(p1, true) + '%\',' + this.escape(p0) + ')';
    }

    $contains(p0: unknown, p1: unknown): string {
        //validate params
        if (p0 == null || p1 == null)
            return '';
        return 'LIKE(\'%' + this.escape(p1, true) + '%\',' + this.escape(p0) + ')';
    }

    $endswith(p0: unknown, p1: unknown): string {
        return this.$endsWith(p0, p1);
    }

    $endsWith(p0: unknown, p1: unknown): string {
        //validate params
        if (p0 == null || p1 == null)
            return '';
        return 'LIKE(\'%' + this.escape(p1, true) + '\',' + this.escape(p0) + ')';
    }

    $day(p0: unknown): string {
        return `CAST(strftime('%d', ${this.escape(p0)}) AS INTEGER)`;
    }

    $dayOfMonth(p0: unknown): string {
        return `CAST(strftime('%d', ${this.escape(p0)}) AS INTEGER)`;
    }

    $month(p0: unknown): string {
        return `CAST(strftime('%m', ${this.escape(p0)}) AS INTEGER)`;
    }

    $year(p0: unknown): string {
        return `CAST(strftime('%Y', ${this.escape(p0)}) AS INTEGER)`;
    }

    $hour(p0: unknown): string {
        return `CAST(strftime('%H', ${this.escape(p0)}) AS INTEGER)`;
    }

    $hours(p0: unknown): string {
        return this.$hour(p0);
    }

    $minute(p0: unknown): string {
        return `CAST(strftime('%M', ${this.escape(p0)}) AS INTEGER)`;
    }

    $minutes(p0: unknown): string {
        return this.$minute(p0);
    }

    $second(p0: unknown): string {
        return `CAST(strftime('%S', ${this.escape(p0)}) AS INTEGER)`;
    }

    $seconds(p0: unknown): string {
        return this.$second(p0);
    }

    $date(p0: unknown): string {
        return 'date(' + this.escape(p0) + ')';
    }

    $ifnull(p0: unknown, p1: unknown): string {
        return this.$ifNull(p0, p1);
    }

    $ifNull(p0: unknown, p1: unknown): string {
        return `IFNULL(${this.escape(p0)}, ${this.escape(p1)})`;
    }

    $toString(p0: unknown) {
        return `CAST(${this.escape(p0)} AS TEXT)`;
    }

    $jsonGet(expr: unknown): string {
        const exprWithName = expr as { $name: string };
        if (typeof exprWithName.$name !== 'string') {
            throw new Error('Invalid json expression. Expected a string');
        }
        const parts = exprWithName.$name.split('.');
        const extract = this.escapeName(parts.splice(0, 2).join('.'));
        return `json_extract(${extract}, '$.${parts.join('.')}')`;
    }

    $jsonEach(expr: unknown): string {
        return `json_each(${this.escapeName(expr as string)})`;
    }

    $uuid(): string {
        return 'uuid4()'
    }

    $toGuid(expr: unknown): string {
        return `uuid_str(crypto_md5(${this.escape(expr)}))`;
    }

    $toInt(expr: unknown): string {
        return `CAST(${this.escape(expr)} AS INT)`;
    }

    $toDouble(expr: unknown): string {
        return this.$toDecimal(expr, 19, 8);
    }

    $toDecimal(expr: unknown, precision?: number, scale?: number): string {
        const p = typeof precision === 'number' ? Math.floor(precision) : 19;
        const s = typeof scale === 'number' ? Math.floor(scale) : 8;
        return `CAST(${this.escape(expr)} as DECIMAL(${p}, ${s}))`
    }

    $toLong(expr: unknown): string {
        return `CAST(${this.escape(expr)} AS BIGINT)`;
    }

    $getDate(type?: 'date' | 'datetime' | 'timestamp'): string {
        switch (type) {
            case 'date':
                return 'date(\'now\')';
            case 'datetime':
                return `strftime('%F %H:%M:%f+00:00', 'now')`;
            case 'timestamp':
                return `STRFTIME('%Y-%m-%d %H:%M:%f', DATETIME('now', 'localtime')) || PRINTF('%+.2d:%.2d', ROUND((JULIANDAY('now', 'localtime') - JULIANDAY('now')) * 24), ABS(ROUND((JULIANDAY('now', 'localtime') - JULIANDAY('now')) * 24 * 60) % 60))`;
            default:
                return `strftime('%F %H:%M:%f+00:00', 'now')`;
        }
    }

    $jsonObject(...expr: unknown[]): string {
        // expected an array of QueryField objects
        const args: string[] = expr.reduce((previous: string[], current: Record<string, unknown>) => {
            if (typeof current === 'string') {
                previous.push(this.escape(current), this.escapeName(current));
                return previous;
            }
            // get the first key of the current object
            let [name] = Object.keys(current);
            let value;
            // if the name is not a string then throw an error
            if (typeof name !== 'string') {
                throw new Error('Invalid json object expression. The attribute name cannot be determined.');
            }
            // if the given name is a dialect function (starts with $) then use the current value as is
            // otherwise create a new QueryField object
            if (name.startsWith('$')) {
                value = new QueryField(current[name]);
                name = value.getName();
            } else {
                value = current instanceof QueryField ? new QueryField(current[name]) : current[name];
            }
            // escape json attribute name and value
            previous.push(this.escape(name), this.escape(value));
            return previous;
        }, []) as string[];
        return `json_object(${args.join(',')})`;;
    }

    $jsonGroupArray(expr: { $jsonGet: unknown[] }): string {
        const [key] = Object.keys(expr);
        if (key !== '$jsonObject') {
            throw new Error('Invalid json group array expression. Expected a json object expression');
        }
        return `json_group_array(${this.escape(expr)})`;
    }

    $jsonArray(expr: QueryField | { $select: Record<string, unknown> } | { $value: unknown } | { $literal: unknown }): string {
        if (expr == null) {
            throw new Error('The given query expression cannot be null');
        }
        if (expr instanceof QueryField) {
            // escape expr as field and waiting for parsing results as json array
            return this.escape(expr);
        }
        // trear expr as select expression
        const exprWithSelect = expr as { $select: Record<string, unknown> };
        if (exprWithSelect.$select) {
            // get select fields
            const args = Object.keys(exprWithSelect.$select).reduce((previous, key) => {
                const select = exprWithSelect.$select[key] as unknown[];
                previous.push(...select);
                return previous;
            }, []);
            const [key] = Object.keys(exprWithSelect.$select);
            // prepare select expression to return json array
            exprWithSelect.$select[key] = [
                {
                    $jsonGroupArray: [ // use json_group_array function
                        {
                            $jsonObject: args // use json_object function
                        }
                    ]
                }
            ];
            return `(${this.format(expr)})`;
        }
        // treat expression as query field
        if (Object.prototype.hasOwnProperty.call(expr, '$name')) {
            return this.escape(expr);
        }
        // treat expression as value
        const exprWithValue = expr as { $value: unknown };
        if (Object.prototype.hasOwnProperty.call(exprWithValue, '$value')) {
            if (Array.isArray(exprWithValue.$value)) {
                return this.escape(JSON.stringify(exprWithValue.$value));
            }
            return this.escape(expr);
        }
        const exprWithLiteral = expr as { $literal: unknown };
        if (Object.prototype.hasOwnProperty.call(exprWithLiteral, '$literal')) {
            if (Array.isArray(exprWithLiteral.$literal)) {
                return this.escape(JSON.stringify(exprWithLiteral.$literal));
            }
            return this.escape(expr);
        }
        throw new Error('Invalid json array expression. Expected a valid select expression');
    }

}

export { LocalSqlFormatter, timezone };