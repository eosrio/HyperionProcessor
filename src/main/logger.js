const cluster = require('cluster');
const log4js = require('log4js');

log4js.configure({
    appenders: {
        stdout: { type: 'stdout' },
    },
    categories: {
        default: { appenders: [ 'stdout' ], level: 'debug' }
    }
});

module.exports = (category) =>
    log4js.getLogger(cluster.isWorker ? 
        `${cluster.worker.id}:${category}` : 
        category);
