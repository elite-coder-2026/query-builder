const { QueryExecutor } = require('./QueryExecutor');
const { validateTable, validateWhere } = require('./utils/validators');

class QueryBuilder {
  constructor(connection, table) {
    this.connection = connection;
    this.table = validateTable(table);
    this.operations = {
      select: ['*'],
      wheres: [],
      joins: [],
      groups: [],
      havings: [],
      orders: [],
      limit: null,
      offset: null,
      inserts: [],
      updates: {},
      deletes: false
    };
    this.logger = connection.logger || console;
  }

  // SELECT methods
  select(...columns) {
    this.operations.select = columns.length ? columns : ['*'];
    return this;
  }

  selectRaw(sql, params = []) {
    this.operations.select = [this.raw(sql, params)];
    return this;
  }

  // WHERE methods - THE CORE OF YOUR CHAINING MAGIC!
  where(field, operator, value) {
    if (value === undefined) {
      value = operator;
      operator = '=';
    }
    
    this.operations.wheres.push({
      type: 'basic',
      field,
      operator,
      value,
      boolean: this.operations.wheres.length ? 'AND' : 'WHERE'
    });
    
    return this;
  }

  orWhere(field, operator, value) {
    if (value === undefined) {
      value = operator;
      operator = '=';
    }
    
    this.operations.wheres.push({
      type: 'basic',
      field,
      operator,
      value,
      boolean: 'OR'
    });
    
    return this;
  }

  whereIn(field, values) {
    this.operations.wheres.push({
      type: 'in',
      field,
      values,
      boolean: this.operations.wheres.length ? 'AND' : 'WHERE'
    });
    return this;
  }

  whereBetween(field, [start, end]) {
    this.operations.wheres.push({
      type: 'between',
      field,
      start,
      end,
      boolean: this.operations.wheres.length ? 'AND' : 'WHERE'
    });
    return this;
  }

  whereNull(field) {
    this.operations.wheres.push({
      type: 'null',
      field,
      boolean: this.operations.wheres.length ? 'AND' : 'WHERE'
    });
    return this;
  }

  whereNotNull(field) {
    this.operations.wheres.push({
      type: 'notNull',
      field,
      boolean: this.operations.wheres.length ? 'AND' : 'WHERE'
    });
    return this;
  }

  // Nested WHERE groups (for complex conditions)
  whereGroup(callback) {
    const groupBuilder = new QueryBuilder(this.connection, this.table);
    callback(groupBuilder);
    
    this.operations.wheres.push({
      type: 'group',
      group: groupBuilder.operations.wheres,
      boolean: this.operations.wheres.length ? 'AND' : 'WHERE'
    });
    
    return this;
  }

  orWhereGroup(callback) {
    const groupBuilder = new QueryBuilder(this.connection, this.table);
    callback(groupBuilder);
    
    this.operations.wheres.push({
      type: 'group',
      group: groupBuilder.operations.wheres,
      boolean: 'OR'
    });
    
    return this;
  }

  // JOIN methods
  join(table, first, operator, second) {
    this.operations.joins.push({
      type: 'INNER',
      table,
      first,
      operator,
      second
    });
    return this;
  }

  leftJoin(table, first, operator, second) {
    this.operations.joins.push({
      type: 'LEFT',
      table,
      first,
      operator,
      second
    });
    return this;
  }

  // ORDER, GROUP, LIMIT
  orderBy(column, direction = 'ASC') {
    this.operations.orders.push({ column, direction });
    return this;
  }

  groupBy(...columns) {
    this.operations.groups.push(...columns);
    return this;
  }

  having(field, operator, value) {
    this.operations.havings.push({ field, operator, value });
    return this;
  }

  limit(limit) {
    this.operations.limit = limit;
    return this;
  }

  offset(offset) {
    this.operations.offset = offset;
    return this;
  }

  // INSERT, UPDATE, DELETE
  insert(data) {
    this.operations.inserts = Array.isArray(data) ? data : [data];
    return this;
  }

  update(data) {
    this.operations.updates = data;
    return this;
  }

  delete() {
    this.operations.deletes = true;
    return this;
  }

  // RAW expressions
  raw(sql, params = []) {
    return { __raw: true, sql, params };
  }

  // EXECUTION - THE MOMENT OF TRUTH!
  async values() {
    const executor = new QueryExecutor(this.connection, this.operations, this.logger);
    
    try {
      // Log query for debugging
      this.logger.debug('Executing query:', executor.toSQL());
      
      const result = await executor.execute();
      
      // Return appropriate result based on operation
      if (this.operations.inserts.length) {
        return { 
          inserted: result.rowCount, 
          ids: result.rows?.map(r => r.id) 
        };
      }
      
      if (this.operations.updates) {
        return { updated: result.rowCount };
      }
      
      if (this.operations.deletes) {
        return { deleted: result.rowCount };
      }
      
      return result.rows;
      
    } catch (error) {
      this.logger.error('Query failed:', error);
      throw new QueryError(`Query failed: ${error.message}`, this.toSQL(), error);
    }
  }

  // Helper methods for common patterns
  async first() {
    this.limit(1);
    const results = await this.values();
    return results[0] || null;
  }

  async count() {
    this.select(this.raw('COUNT(*) as count'));
    const result = await this.first();
    return result?.count || 0;
  }

  async exists() {
    const count = await this.count();
    return count > 0;
  }

  async pluck(column) {
    const results = await this.select(column).values();
    return results.map(r => r[column]);
  }

  // Debug - get SQL without executing
  toSQL() {
    const executor = new QueryExecutor(this.connection, this.operations);
    return executor.toSQL();
  }
}

class QueryError extends Error {
  constructor(message, sql, originalError) {
    super(message);
    this.sql = sql;
    this.originalError = originalError;
    this.name = 'QueryError';
  }
}

module.exports = { QueryBuilder, QueryError };