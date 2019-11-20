const nodemailer = require('nodemailer');
const config = require('./config/config');
const logger = require('./lib/logger');
const mailTransporterConfig = config.mailTransporter;
const transporter = nodemailer.createTransport(mailTransporterConfig);

const {
    from,
    subject
} = config.mailFormat;

class EmailClientService {
    constructor (logger, transporter) {
        this.logger = logger;
        this.transporter = transporter;
    }

    /**
     * @param email
     * @param message
     * @param reqId
     * @returns {Promise<T | never>}
     */
    sendEmail (email, message, reqId) {
        this.logger.info(`Request ID: ${reqId} - sending message to email: ${email}`);

        return this.transporter.sendMail({
            from,
            to: email,
            subject,
            text: message,
            html: message
        })
            .then(() => {
                this.logger.info(`Request ID: ${reqId} - successfully sent message to email: ${email}`);
            }).catch(error => {
                this.logger.error(`Request ID: ${reqId} - error sending message to email: ${email}. Error: ${error}`);
            });
    }
}

module.exports = new EmailClientService(logger, transporter);