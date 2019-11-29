'use strict';

function createTimestampForSQLDatabase() {
    // courtesy: https://stackoverflow.com/questions/5129624/convert-js-date-time-to-mysql-datetime
    return new Date().toISOString().slice(0, 19).replace('T', ' ');
}

module.exports = {
    createTimestampForSQLDatabase
};