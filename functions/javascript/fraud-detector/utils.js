'use strict';

const startingIndexOfTime = 0;
const endingIndexOfTime = 19;

// courtesy: https://stackoverflow.com/questions/5129624/convert-js-date-time-to-mysql-datetime
const createTimestampForSQLDatabase = () => new Date().toISOString().slice(startingIndexOfTime, endingIndexOfTime).replace('T', ' ');

module.exports = {
    createTimestampForSQLDatabase
};
