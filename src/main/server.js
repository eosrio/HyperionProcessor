const log = require('./logger')('master');

const createConnection = require('./queue/connection');

const createConsumer = require('./queue/consumer');

const createIndexer = require('./worker/indexer');

const { Client } = require('elasticsearch');

const cluster = require('cluster');

module.exports = (argv) => {
    argv.amqp.timeout = +argv.amqp.timeout;
    argv.amqp.batch = +argv.amqp.batch;
    if (cluster.isMaster) {
        argv.amqp.url = `amqp://${argv.amqp.username}:${argv.amqp.password}@${argv.amqp.host}/%2F${argv.amqp.vhost}`;
//        argv.elasticsearch.httpAuth = `${argv.elasticsearch.username}:${argv.elasticsearch.password}`;
        configs(argv.amqp).forEach(config => {
            const worker = cluster.fork({
                WORKER_CONFIG: JSON.stringify(config),
                WORKER_ARGV: JSON.stringify(argv)
            });
            worker.on('disconnect', worker => log.warn(`The worker #${worker.id} has disconnected`));
        })
    } else {
        const config = JSON.parse(process.env.WORKER_CONFIG);
        const argv = JSON.parse(process.env.WORKER_ARGV);
	console.log(config);
        let createWorker = config.worker;

        if (config.worker.index) {
            config.worker.client = new Client(argv.elasticsearch);
            createWorker = () => createIndexer(config.worker);
        } else {
            const createProcessor = require(`./worker/${config.worker.processor}`);
            createWorker = () => createProcessor(config.worker);
        }

        createConnection(argv.amqp.url).
            then(connection => {
                config.connection = connection;
                const stream = createConsumer(config);
                const doWork = createWorker();
                stream.subscribe(val => doWork(val));
            });
    }
}

function configs(argv) {
    return [
        {
           exchange: 'eosio',
            routingKey: 'action_trace',
            queue: 'index:action_trace',
            batch: argv.batch,
            worker: {
                index: 'eosio-action_trace',
                batch: argv.batch,
                timeout: argv.timeout
            }
        },
        {
            exchange: 'eosio',
            routingKey: 'accepted_block',
            queue: 'index:block',
            batch: argv.batch,
            worker: {
                index: 'eosio-block',
                batch: argv.batch,
                timeout: argv.timeout
            }
        },
        {
            exchange: 'eosio',
            routingKey: ['irreversible_block', 'fork', 'failed_tx'],
            queue: 'index:etc',
            batch: argv.batch,
            worker: {
                index: 'eosio-etc',
                batch: argv.batch,
                timeout: argv.timeout
            }
        },
        {
            exchange: 'eosio',
            routingKey: 'action_trace',
            queue: 'process:action_trace',
            batch: argv.batch,
            worker: {
                processor: 'action-trace',
                exchange: 'eosio'
            }
        },
        {
            exchange: 'eosio',
            routingKey: 'action',
            queue: 'index:action',
            batch: argv.batch,
            worker: {
                index: 'eosio-action',
                batch: argv.batch,
                timeout: argv.timeout
            }
        }/*,
        {
            exchange: 'eosio',
            routingKey: ['resource_balance', 'currency_balance'],
            queue: 'index:balance',
            batch: argv.batch,
            worker: {
                index: 'eosio-balance',
                batch: argv.batch,
                timeout: argv.timeout
            }
        }*/
    ]
}
