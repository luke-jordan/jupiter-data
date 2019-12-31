'use strict';

// We use the https library to confirm the SNS subscription
const https = require('https');
const { PubSub } = require('@google-cloud/pubsub');
const pubsub = new PubSub();
const topicName = 'sns-events';
const authValidator = require('./libs/auth-validator');
const logger = require('debug')('jupiter:sns-to-pubsub');
const httpStatus = require('http-status');
const { httpMethods, SUBSCRIPTION_CONFIRMATION } = require('./constants');

const {
    POST
} = httpMethods;

const publishMessageToPubSub = async (message, res) => {
    try {
        logger('Converting message to buffer');

        /**
         * `neededParams` is sent as array as the function that would load the data into big query expects an array
         * The keys: `user_id`, `event_type`, `timestamp` and `context match the schema of `all_user_events` table in big query
         * Please do not change the key names in `neededParams` without updating the schema of all_user_events table in big query
         */
        /* eslint-disable camelcase */
        const neededParams = [{
            user_id: message.userId,
            event_type: message.eventType,
            time_transaction_occurred: message.timestamp,
            context: message.context ? JSON.stringify(message.context) : ''
        }];
        /* eslint-enable camelcase */
        const msgData = Buffer.from(JSON.stringify(neededParams));

        logger(`Sending message to pub/sub. Buffered message: ${JSON.stringify(msgData)}`);
        const messageId = await pubsub.topic(topicName).publish(msgData);
        logger(`Message published with id: ${messageId}`);
        res.status(httpStatus.OK).end(messageId);
    } catch (error) {
        logger(`Error while publishing to pub/sub. Error: ${JSON.stringify(error.message)}`);
        throw error;
    }
};

const confirmSubscriptionToBroker = async (message, res) => {
    logger(`Confirming subscription at url: ${message.SubscribeURL}`);
    // SNS subscriptions are confirmed by requesting the special URL sent
    // by the service as a confirmation
    https.get(message.SubscribeURL, (subRes) => {
        logger(`statusCode: ${subRes.statusCode}`);
        logger(`headers: ${JSON.stringify(subRes.headers)}`);

        subRes.on('data', (data) => {
            logger(data);
            res.status(httpStatus.OK).end('ok');
        });
    }).on('error', (err) => {
        logger(`Error occurred while confirming subscription at url: ${message.SubscribeURL}.
        Error: ${JSON.stringify(err.message)}`);
        res.status(httpStatus.INTERNAL_SERVER_ERROR).end('Subscription Confirmation failed');
    });
};

const processMessageBasedOnType = async (message, res) => {
    if (message && message.eventType) {
        await publishMessageToPubSub(message, res);
        return;
    }

    // here we handle either a request to confirm subscription
    if (message.Type && message.Type.toLowerCase() === SUBSCRIPTION_CONFIRMATION) {
        await confirmSubscriptionToBroker(message, res);
    }
};

const receiveNotification = async (req, res) => {
    if (req.method !== POST) {
        res.status(httpStatus.METHOD_NOT_ALLOWED).end(`Only ${POST} http method accepted`);
        return;
    }

    // all valid SNS requests should have this header
    const snsHeader = req.get('x-amz-sns-message-type');
    if (!snsHeader) {
        res.status(httpStatus.UNAUTHORIZED).end('Invalid SNS message => at amz header');
        return;
    }

    try {
        const message = JSON.parse(req.body);
        logger(`JSON parsed message received: ${JSON.stringify(message)}`);
        authValidator(message.hash, message.eventType);
        await processMessageBasedOnType(message, res);
        return;
    } catch (error) {
        logger(`Error occurred while sending sns to pubsub. Error: ${JSON.stringify(error.stack)}`);
        res.status(httpStatus.BAD_REQUEST).end('Invalid SNS message => authentication or processing error');
    }
};

module.exports = {
    receiveNotification
};
