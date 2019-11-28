'use strict';

const emailClientService = require('./email-client');
const logger = require('debug')('jupiter:notification-service');
const shortid = require('shortid');
const constants = require('./constants');
const httpStatus = require('http-status');

const {
    notificationTypes,
    httpMethods
} = constants;

const { EMAIL_TYPE } = notificationTypes;
const { POST } = httpMethods;

const sendEmailNotifications = (payload, options) => {
    const {
        contacts, message
    } = payload;
    const {
        reqId
    } = options;
    logger(`Request ID: ${reqId} - send message: ${message} via email to contacts: ${JSON.stringify(contacts)}`);
    contacts.forEach((contact) => emailClientService.sendEmail(contact, message, reqId));
};

const sendMessageBasedOnType = async (payload, options) => {
    const {
        notificationType, message
    } = payload;
    const {
        reqId
    } = options;
    logger(`Request ID: ${reqId} - sending message: ${message} based on notification type: ${notificationType}`);

    switch (notificationType) {
        case EMAIL_TYPE:
            await sendEmailNotifications(payload, options);
            return;
        default:
            throw new Error('Notification Type not supported at the moment');
    }
};

const sendMessageToContacts = async (payload, reqId) => {
    const {
        contacts, message
    } = payload;
    const options = {
        reqId
    };

    logger(`Request ID: ${reqId} - sending message: ${message} to contacts: ${JSON.stringify(contacts)}`);
    await sendMessageBasedOnType(payload, options);
};

const missingParameterInReceivedPayload = (parameters) => !parameters.notificationType || !parameters.contacts ||
    !parameters.message || !Array.isArray(parameters.contacts) || parameters.contacts.length === 0;

const handleMissingParameterInReceivedPayload = (payload, res, reqId) => {
    res.status(httpStatus.BAD_REQUEST).end(`invalid payload => 'notificationType', 'contacts' and 'message' are required`);
    logger(
        `Request ID: ${reqId} - request to send notification failed because of invalid parameters in received payload. Received payload: ${JSON.stringify(payload)}`
    );
    return;
};

const handleNotSupportedHttpMethod = (res) => {
    res.status(httpStatus.METHOD_NOT_ALLOWED).end(`only ${POST} http method accepted`);
    return;
};

const sendSuccessResponse = (res, reqId) => {
    logger(`Request ID: ${reqId} - successfully handled send notification request`);
    res.status(httpStatus.OK).json('Successfully sent notification request');
    return;
};

const sendFailureResponse = (res, error, reqId) => {
    logger(`Request ID: ${reqId} - error occurred while handling send notification request. Error: ${JSON.stringify(error)}`);
    res.status(400).end('Unable to send notification request');
    return;
};

const handleSendNotificationRequest = async (req, res) => {
    if (req.method !== POST) {
        return handleNotSupportedHttpMethod(res);
    }

    const reqId = shortid.generate();
    logger(`Request ID: ${reqId} - handling send notification request with raw payload: ${JSON.stringify(req.body)}`);
    try {
        const payload = JSON.parse(JSON.stringify(req.body));
        logger(`Request ID: ${reqId} - successfully parsed received payload`);

        if (missingParameterInReceivedPayload(payload)) {
            return handleMissingParameterInReceivedPayload(payload, res, reqId);
        }

        await sendMessageToContacts(payload, reqId);

        return sendSuccessResponse(res, reqId);
    } catch (error) {
        sendFailureResponse(res, error, reqId);
    }
};

module.exports = {
    sendEmailNotifications,
    sendMessageBasedOnType,
    sendMessageToContacts,
    missingParameterInReceivedPayload,
    handleSendNotificationRequest
};

// TODO: deploy service => add to circle ci and terraform (supply with working email config)