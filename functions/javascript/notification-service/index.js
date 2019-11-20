'use strict';

const emailClientService = require('./email_client');
const logger = require('./lib/logger');
const shortid = require('shortid');
const constants = require('./config/constants');

const { EMAIL_TYPE } = constants.notificationTypes;

const sendMessageToContact = async (payload, reqId) => {
    const {
        notificationType, contacts, message
    } = payload;

    logger.info(`Request ID : ${reqId} - sending message: ${message} to contacts: ${JSON.stringify(contacts)}`);

    switch (notificationType) {
    case EMAIL_TYPE:
        return contacts.map(contact => emailClientService.sendEmail(contact, message, reqId));
    default:
        throw new Error('Notifiation Type not supported at the moment');
    }
}

const missingParameter= (parameters) => {
    return !parameters.notificationType || !parameters.contacts 
    || !parameters.message || !Array.isArray(parameters.contacts);
}

const handleSendNotificationRequest = async (req, res) => {
    if (req.method !== 'POST') {
        res.status(405).end('only post method accepted');
        return;
    }

    try {
        const payload = JSON.parse(JSON.stringify(req.body));
        const reqId = shortid.generate();

        logger.info(`Request ID : ${reqId} - payload received: ${JSON.stringify(payload)}`);

        if (missingParameter(payload)) {
            res.status(400).end(`invalid payload => 'notificationType', 'contacts' and 'message' are required`);
            logger.info
            (`Request ID : ${reqId} - request to send notification failed because of invalid parameters in received payload: ${JSON.stringify(payload)}`
            );
            return;
        }

        await sendMessageToContact(payload, reqId);

        logger.info(`Request ID : ${reqId} - successfully handled send notification request`);
        res.status(200).json(rows);
        return;
    } catch (error) {
        logger.error(`Request ID : ${reqId} - error occurred while handling send notification request. Error: ${JSON.stringify(error)}`);
        res.status(400).end('Unable to send notification request');
    }
}


async function test() {
    const payload = {
        notificationType: 'EMAIL', 
        contacts: ['bolu@gmail.com', 'john@gmail.com'], 
        message: "hello world"
    };

    const reqId = 'dlfkjdkf';
    await sendMessageToContact(payload, reqId);
    console.log('done');
}

test();
