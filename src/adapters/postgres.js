const { Pool } = require('pg');

class PostgresAdapter {
    static async connect(config) {
        const pool = new Pool({
            host: config.host,
            port: config.port,
            database: config.database,
            user: config.user,
            password: config.password,
            max: config.pool?.max || 20,
            idleTimeoutMillis: config.pool?.idle || 30000
        });

        // Test connection
        await pool.query('SELECT 1');

        // Decorate pool with query method
        pool.query = async (sql, params) => {
            try {
                const result = await pool.query(sql, params);
                return {
                    rows: result.rows,
                    rowCount: result.rowCount
                };
            } catch (error) {
                throw new Error(`Postgres error: ${error.message}`);
            }
        };

        return pool;
    }
}

module.exports = PostgresAdapter;