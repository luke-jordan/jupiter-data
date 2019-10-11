'use strict';

// We use the https library to confirm the SNS subscription
const https = require('https');

// import the Google Cloud Pubsub client library
const { PubSub } = require('@google-cloud/pubsub');

// our pubsub client
const pubsub = new PubSub();

// the cloud pubsub topic we will publish messages to
const topicName = 'sns-events';

/**
 * Cloud Function.
 * @param {req} request The web request from SNS.
 * @param {res} The response returned from this function.
 */
exports.receiveNotification  = function receiveNotification (req, res) {
    // we only respond to POST method HTTP requests
    if (req.method !== 'POST') {
        res.status(405).end('only post method accepted');
        return;
    }

    // all valid SNS requests should have this header
    const snsHeader = req.get('x-amz-sns-message-type');
    if (snsHeader === undefined) {
        res.status(403).end('invalid SNS message => at amz header');
        return;
    }

    (async function() {
        const message = JSON.parse(req.body);
        console.log('JSON parsed message received: ', message);

        if (message && message.eventType) {
            const attributes = {
                transactionId: (message.context ? message.context.transactionId : '')
            };
            try {
                console.log('converting message to buffer');
                const msgData = Buffer.from(`userid: ${message.userId} performed event type: ${message.eventType} at timestamp: ${message.timestamp}`);

                console.log('sending message to pub/sub. Buffered message:', msgData);
                const messageId = await pubsub.topic(topicName).publish(msgData, attributes);

                console.log(`message published with id: ${messageId}`);
                res.status(200).end(messageId);
                return;
            } catch(error) {
                console.error('error while publishing to pub/sub', error);
            }
        }

        // here we handle either a request to confirm subscription
        if (message.Type && message.Type.toLowerCase() === 'subscriptionconfirmation') {
            console.log(`confirming subscription: ${message.SubscribeURL}`);
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
                res.status(500).end('confirmation failed');
            });
            return;
        }

        console.error('should not have gotten to default block');
        res.status(400).end('invalid SNS message');

    })();
};


