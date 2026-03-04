class QueryExecutor {
    constructor(connection, operations, logger) {
        this.connection = connection;
        this.ops = operations;
        this.logger = logger;
        this.params = [];
    }

    toSQL() {
        if (this.ops.inserts.length) {
            return this.buildInsertSQL();
        }
        if (this.ops.updates) {
            return this.buildUpdateSQL();
        }
        if (this.ops.deletes) {
            return this.buildDeleteSQL();
        }
        return this.buildSelectSQL();
    }

    buildSelectSQL() {
        let sql = `SELECT ${this.ops.select.join(', ')} FROM ${this.table}`;

        // JOINS
        for (const join of this.ops.joins) {
            sql += ` ${join.type} JOIN ${join.table} ON ${join.first} ${join.operator} ${join.second}`;
        }

        // WHERE
        if (this.ops.wheres.length) {
            sql += this.buildWhereClause();
        }

        // GROUP BY
        if (this.ops.groups.length) {
            sql += ` GROUP BY ${this.ops.groups.join(', ')}`;
        }

        // HAVING
        if (this.ops.havings.length) {
            sql += ' HAVING ' + this.ops.havings.map(h =>
                `${h.field} ${h.operator} ?`
            ).join(' AND ');
            this.params.push(...this.ops.havings.map(h => h.value));
        }

        // ORDER BY
        if (this.ops.orders.length) {
            sql += ' ORDER BY ' + this.ops.orders.map(o =>
                `${o.column} ${o.direction}`
            ).join(', ');
        }

        // LIMIT & OFFSET
        if (this.ops.limit) {
            sql += ` LIMIT ${this.ops.limit}`;
        }
        if (this.ops.offset) {
            sql += ` OFFSET ${this.ops.offset}`;
        }

        return { sql, params: this.params };
    }

    buildWhereClause() {
        let sql = '';

        for (const where of this.ops.wheres) {
            sql += where.boolean + ' ';

            if (where.type === 'group') {
                sql += '(';
                let groupSql = '';
                for (const groupWhere of where.group) {
                    groupSql += groupWhere.boolean + ' ';
                    groupSql += this.buildWhereCondition(groupWhere);
                }
                sql += groupSql.slice(where.group[0].boolean.length);
                sql += ') ';
            } else {
                sql += this.buildWhereCondition(where) + ' ';
            }
        }

        return ' WHERE ' + sql.slice(this.ops.wheres[0].boolean.length, -1);
    }

    buildWhereCondition(where) {
        switch (where.type) {
            case 'basic':
                this.params.push(where.value);
                return `${where.field} ${where.operator} ?`;
            case 'in':
                this.params.push(...where.values);
                return `${where.field} IN (${where.values.map(() => '?').join(', ')})`;
            case 'between':
                this.params.push(where.start, where.end);
                return `${where.field} BETWEEN ? AND ?`;
            case 'null':
                return `${where.field} IS NULL`;
            case 'notNull':
                return `${where.field} IS NOT NULL`;
            default:
                return '';
        }
    }

    async execute() {
        const { sql, params } = this.toSQL();

        if (this.logger) {
            this.logger.debug('Executing:', sql, params);
        }

        return this.connection.query(sql, params);
    }
}

module.exports = { QueryExecutor };