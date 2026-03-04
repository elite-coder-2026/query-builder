const adapters = {
    postgres: require('./adapters/postgres'),
    mysql: require('./adapters/mysql'),
    sqlite: require('./adapters/sqlite'),
    mongodb: require('./adapters/mongodb')
};

class DatabaseFactory {
    static async createConnection(config) {
        const adapter = adapters[config.client];

        if (!adapter) {
            throw new Error(`Unsupported database client: ${config.client}`);
        }

        const connection = await adapter.connect(config);

        // Decorate connection with logger
        connection.logger = config.logger || console;

        // Add transaction support
        connection.beginTransaction = async () => {
            const client = await connection.getConnection();
            await client.query('BEGIN');
            return client;
        };

        return connection;
    }
}

module.exports = DatabaseFactory;