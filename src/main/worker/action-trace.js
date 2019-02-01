const { omit, merge } = require('lodash');
const { Observable } = require('rxjs/Observable');

const REGEX = /^([0-9]+(?:\.[0-9]+)?)\s([A-ZAa-z0-9]+)$/;

module.exports = function createActionSplitter(config) {
    return (payload) => {
        const action = payload.body.action_trace;
        try {
            action.action_trace.inline_traces.
                forEach(child => publishActions(child, action.action_trace, payload.channel));
        } finally {
            payload.channel.ack(payload.message);
        }
    };

    function publishActions(action, parent, channel) {
        delete action.act.hex_data;
        const traces = action.inline_traces;
	// console.log(traces);
        action = omit(action, 'inline_traces'),

        publish(channel, {
            id: action.receipt.global_sequence,
            '@timestamp': `${action.block_time}Z`,
            type: 'action',
            action: {
                action,
                parent: {
                    root: false,
                    action_id: parent.receipt.global_sequence
                }
            }
        });

        traces.forEach(child => publishActions(child, action, channel));
    }

    function publish(channel, object) {
        channel.publish(
		config.exchange,
		object.type,
		Buffer.from(JSON.stringify(object)), {
			contentType: "application/json"
	});
    }
}
