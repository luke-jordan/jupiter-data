'use strict';

const dotenv = require('dotenv');
dotenv.config();

module.exports = {
    mailTransporter: {
        host: process.env.EMAIL_HOST,
        port: process.env.EMAIL_PORT,
        secure: process.env.EMAIL_SECURITY, // true for 465, false for other ports
        auth: {
            user: process.env.EMAIL_ADDRESS,
            pass: process.env.EMAIL_PASSWORD
        }
    },
    mailFormat: {
        from: `"Jupyter Save 👻" ${process.env.EMAIL_ADDRESS}`,
        subject: `Notification`
    }
};
