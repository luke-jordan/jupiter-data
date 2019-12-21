'use strict';

// We use the https library to confirm the SNS subscription
const https = require('https');

// import the Google Cloud Pubsub client library
const { PubSub } = require('@google-cloud/pubsub');

// our pubsub client
const pubsub = new PubSub();

// the cloud pubsub topic we will publish messages to
const topicName = 'sns-events';

const authValidator = require('./libs/auth-validator');

async function processMessageBasedOnType(message, res) {
    if (message && message.eventType) {
        try {
            console.log('Converting message to buffer');
            /**
             * `neededParams` is sent as array as the function that would load the data into big query expects an array
             * The keys: `user_id`, `event_type`, `timestamp` and `context match the schema of `all_user_events` table in big query
             * Please do not change the key names in `neededParams` without updating the schema of all_user_events table in big query
            */
            const neededParams = [{
                user_id: message.userId,
                event_type: message.eventType,
                time_transaction_occurred: message.timestamp,
                context: (message.context ? JSON.stringify(message.context) : ''),
            }];
            const msgData = Buffer.from(JSON.stringify(neededParams));

            console.log('Sending message to pub/sub. Buffered message:', msgData);
            const messageId = await pubsub.topic(topicName).publish(msgData);
            console.log(`Message published with id: ${messageId}`);
            res.status(200).end(messageId);
            return;
        } catch(error) {
            console.error('Error while publishing to pub/sub', error);
        }
    }

    // here we handle either a request to confirm subscription
    if (message.Type && message.Type.toLowerCase() === 'subscriptionconfirmation') {
        console.log(`Confirming subscription: ${message.SubscribeURL}`);
        // SNS subscriptions are confirmed by requesting the special URL sent
        // by the service as a confirmation
        https.get(message.SubscribeURL, (subRes) => {
            console.log('statusCode:', subRes.statusCode);
            console.log('headers:', subRes.headers);

            subRes.on('data', (d) => {
                console.log(d);
                res.status(200).end('ok');
            });
        }).on('error', (e) => {
            console.error(e);
            res.status(500).end('Confirmation failed');
        });
        return;
    }
}

/**
 * Cloud Function.
 * @param {req} request The web request from SNS.
 * @param {res} The response returned from this function.
 */
exports.receiveNotification  = function receiveNotification (req, res) {
    // we only respond to POST method HTTP requests
    if (req.method !== 'POST') {
        res.status(405).end('Only post method accepted');
        return;
    }

    // all valid SNS requests should have this header
    const snsHeader = req.get('x-amz-sns-message-type');
    if (snsHeader === undefined) {
        res.status(403).end('Invalid SNS message => at amz header');
        return;
    }

    try {
        const message = JSON.parse(req.body);
        console.log('JSON parsed message received: ', message);
        authValidator(message.hash, message.eventType);
        return processMessageBasedOnType(message, res);
    } catch(error) {
        console.error('Error occurred while sending sns to pubsub. Error: ', error);
        res.status(400).end('Invalid SNS message => authentication or processing error');
    }
};
