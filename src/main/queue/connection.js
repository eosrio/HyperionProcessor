const log = require('../logger')('queue.connection');
const amqplib = require('amqplib');
const { isFatalError } = require('amqplib/lib/connection');

module.exports = function createConnection(uri) {
    log.info('connecting');
    return amqplib.connect(uri).
        then(connection => {
            log.info('connected');
            setupListeners(connection);
            return connection;
        });
};

function setupListeners(connection) {
    connection.on('close', () => log.info('disconnected'));
    connection.on('error', (err) => {
        if (isFatalError(err)) {
            log.fatal(err.stack);
            process.disconnect();
        } else {
            log.error(err.stack);
        }
    });
}

