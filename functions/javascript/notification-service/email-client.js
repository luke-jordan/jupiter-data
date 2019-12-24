'use strict';

const nodemailer = require('nodemailer');
const config = require('config');
const logger = require('debug')('notification-service:email-client');
const mailTransporterConfig = config.get('mailTransporter');
const transporter = nodemailer.createTransport(mailTransporterConfig);

const {
    from
} = config.get('mailFormat');

const ENVIRONMENT = config.get('environment');

/**
 * @param email
 * @param message
 * @param reqId
 * @returns {Promise<T | never>}
 */
const sendEmail = (payload, reqId) => {
    const {
        email,
        message,
        subject
    } = payload;
    logger(`Request ID: ${reqId} - sending message to email: ${email}`);
    return transporter.sendMail({
        from,
        to: email,
        subject: `${ENVIRONMENT} - ${subject}`,
        text: message,
        html: message
    }).
        then(() => {
            logger(`Request ID: ${reqId} - successfully sent message to email: ${email}`);
        }).catch((error) => {
            logger(`Request ID: ${reqId} - error sending message to email: ${email}. Error: ${error}`);
        });
};

module.exports = {
    sendEmail
};
