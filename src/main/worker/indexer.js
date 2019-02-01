const log = require('../logger')('indexer');
const asyncTimedCargo = require('async-timed-cargo');
const _ = require('lodash');
const hash = require('object-hash');
const { encode } = require('urlsafe-base64');

module.exports = function createIndexer(config) {
    const client = config.client;
    const indexer = _createIndexer(config);
    const queue = asyncTimedCargo(indexer, config.batch, config.timeout);

    return (payload) => queue.push(payload);

    function _createIndexer(config) {
        return (payloads, cb) => {
            //log.info(`bulk ${config.index} ${payloads.length}`);
            const channel = payloads[0].channel;
            const messageMap = {};
            client.bulk({
                index: config.index, type: '_doc',
                body: toBulkPayload(payloads, messageMap)
            }).then(resp => {
                try {
                    if (resp.errors) {
			console.log(resp.items[0]);
                        ackOrNack(resp, messageMap, channel);
                        return;
                    }
                    channel.ackAll();
                    //log.info('ack all');
                } finally {
                    cb();
                }
            }).catch(err => {
                try {
                    channel.nackAll();
                    log.error('nack all', err.stack);
                } finally {
                    cb();
                }
            });
        }
    }
}

function toBulkPayload(payloads, messageMap) {
    return _(payloads).
        map(payload => {
            const body = payload.body;
		if(body.type === 'action') {


			if(!body.action.action.act.data) {
				console.log(body.action.action.act);
			}
			delete body.action.action.receipt.auth_sequence;
			if(body.action.action.act.data.result) delete body.action.action.act.data.result;
			if(body.action.action.act.data.bet) delete body.action.action.act.data.bet;
			if(body.action.action.act.data.owner) delete body.action.action.act.data.owner;
		} else if (body.type === 'action_trace') {
			// console.log(body.action_trace.action_trace.inline_traces);
			delete body.action_trace.action_trace.receipt.auth_sequence;
			if(body.action_trace.action_trace.inline_traces) delete body.action_trace.action_trace.inline_traces;
		}
            body.id = body.id || encode(hash(body, {
                unorderedObjects: false,
                encoding: 'buffer'
            }));

            messageMap[body.id] = payload.message;

            return [ { index: { _id: body.id } }, body ]
        }).flatten().value();
}

function ackOrNack(resp, messageMap, channel) {
    resp.items.forEach(item => {
        const message = messageMap[item.index._id];
        delete messageMap[item.index._id];
        if (item.index.status !== 201 && item.index.status !== 200) {
            channel.nack(message);
	    console.log(item);
            log.info(`nack ${item.index._id} ${item.index.status}`);
        }
        else {
            channel.ack(message);
            // log.info(`ack ${item.index._id}`);
        }
    });
}
