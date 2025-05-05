import {SqlFormatter} from '@themost/query';

class LocalSqlFormatter extends SqlFormatter {
    static readonly NAME_FORMAT = '"$1"';
    constructor() {
        super();
        this.settings.nameFormat = LocalSqlFormatter.NAME_FORMAT
    }
}

export { LocalSqlFormatter };