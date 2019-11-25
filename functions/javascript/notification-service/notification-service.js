'use strict';

const emailClientService = require('./email-client');
const logger = require('debug')('notification-service:notification-service');
const shortid = require('shortid');
const constants = require('./constants');
const httpStatus = require('http-status');

const { EMAIL_TYPE } = constants.notificationTypes;

const sendMessageToContact = async (payload, reqId) => {
    const {
        notificationType, contacts, message
    } = payload;

    logger(`Request ID : ${reqId} - sending message: ${message} to contacts: ${JSON.stringify(contacts)}`);

    switch (notificationType) {
    case EMAIL_TYPE:
        return contacts.map((contact) => emailClientService.sendEmail(contact, message, reqId));
    default:
        throw new Error('Notifiation Type not supported at the moment');
    }
};

const missingParameter = (parameters) => !parameters.notificationType || !parameters.contacts || 
    !parameters.message || !Array.isArray(parameters.contacts);

const handleSendNotificationRequest = async (req, res) => {
    if (req.method !== 'POST') {
        res.status(httpStatus.METHOD_NOT_ALLOWED).end('only post method accepted');
        return;
    }
    const reqId = shortid.generate();    
    logger(`Request ID : ${reqId} - handling send notification request with raw payload: ${JSON.stringify(req.body)}`);
    try {
        const payload = JSON.parse(JSON.stringify(req.body));

        logger(`Request ID : ${reqId} - successfully parsed received payload`);

        if (missingParameter(payload)) {
            res.status(httpStatus.BAD_REQUEST).end(`invalid payload => 'notificationType', 'contacts' and 'message' are required`);
            logger(`Request ID : ${reqId} - request to send notification failed because of invalid parameters in received payload: ${JSON.stringify(payload)}`
            );
            return;
        }

        await sendMessageToContact(payload, reqId);

        logger(`Request ID : ${reqId} - successfully handled send notification request`);
        res.status(httpStatus.OK).json('Successfully sent notification request');
        return;
    } catch (error) {
        logger(`Request ID : ${reqId} - error occurred while handling send notification request. Error: ${JSON.stringify(error)}`);
        res.status(400).end('Unable to send notification request');
    }
};

// TODO: deploy service => add to circle ci and terraform