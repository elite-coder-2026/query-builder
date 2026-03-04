const { QueryBuilder } = require('./QueryBuilder');
const { createConnection } = require('./db');
const { MigrationManager } = require('./migrations/MigrationManager');

class Database {
  constructor(config) {
    this.config = config;
    this.connection = null;
  }

  async connect() {
    this.connection = await createConnection(this.config);
    return this;
  }

  from(table) {
    return new QueryBuilder(this.connection, table);
  }

  async migrate() {
    const migrator = new MigrationManager(this.connection);
    return migrator.run();
  }

  async seed(seeds) {
    // Run seed files
    for (const seed of seeds) {
      await this.from(seed.table).insert(seed.data).values();
    }
  }

  async transaction(callback) {
    const trx = await this.connection.beginTransaction();
    try {
      const result = await callback(new QueryBuilder(trx));
      await trx.commit();
      return result;
    } catch (error) {
      await trx.rollback();
      throw error;
    }
  }
}

module.exports = Database;