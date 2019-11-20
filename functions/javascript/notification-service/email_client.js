'use strict';

const nodemailer = require('nodemailer');
const config = require('./config/config');
const logger = require('./lib/logger');
const mailTransporterConfig = config.mailTransporter;
const transporter = nodemailer.createTransport(mailTransporterConfig);

const {
    from,
    subject
} = config.mailFormat;

/**
 * @param email
 * @param message
 * @param reqId
 * @returns {Promise<T | never>}
 */
const sendEmail = (email, message, reqId) => {
    logger.info(`Request ID: ${reqId} - sending message to email: ${email}`);
    return transporter.sendMail({
        from,
        to: email,
        subject,
        text: message,
        html: message
    }).
        then(() => {
            logger.info(`Request ID: ${reqId} - successfully sent message to email: ${email}`);
        }).catch((error) => {
            logger.error(`Request ID: ${reqId} - error sending message to email: ${email}. Error: ${error}`);
        });
};

module.exports = {
    sendEmail
};
