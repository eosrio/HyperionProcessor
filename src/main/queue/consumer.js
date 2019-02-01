const log = require('../logger')('queue.consumer');
const { isArray } = require('lodash');
const { Observable } = require('rxjs/Observable');

module.exports = function createConsumer(config) {
    return Observable.create(observer =>
        config.connection.createChannel().
            then(channel => Promise.all([
                channel.assertQueue(config.queue, { durable: true }),
                channel.assertExchange(config.exchange, 'topic')
            ]).
                then(() => Promise.all(toArray(config.routingKey).map(routingKey =>
                    channel.bindQueue(config.queue, config.exchange, routingKey)))).
                then(() => {
                    setupListeners(channel);
                    channel.prefetch(config.batch);
                    channel.consume(config.queue, (message) => {
                        try {
                            observer.next({
                                message, channel,
                                body: JSON.parse(message.content)
                            });
                        } catch (err) {
                            observer.error(err);
                        }
                    });
                })));
};

function setupListeners(channel) {
    channel.on('error', (err) => {
        log.fatal(err.stack);
        process.disconnect ? process.disconnect() : process.exit(1);
    });
}

function toArray(val) {
    if (!isArray(val))
        val = [ val ];
    return val;
}
