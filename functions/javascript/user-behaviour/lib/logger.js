'use strict';

const { createLogger, format, transports } = require('winston');
// eslint-disable-next-line no-shadow
const { combine, timestamp, printf } = format;

// eslint-disable-next-line no-shadow
const myFormat = printf(({ level, message, timestamp }) => `${timestamp} ${level.toUpperCase()}: ${message}`);

const logger = createLogger({
    format: combine(
        timestamp(),
        myFormat
    ),
    transports: [new transports.Console()]
});

module.exports = logger;
