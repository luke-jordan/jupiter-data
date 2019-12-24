'use strict';

module.exports = {
    accuracyStates: {
        pending: 'PENDING_CONFIRMATION',
        falseAlarm: 'FALSE_ALARM',
        accuratePrediction: 'ACCURATE_PREDICTION'
    },
    notificationTypes: {
        EMAIL_TYPE: 'EMAIL'
    },
    httpMethods: {
        POST: 'POST',
        GET: 'GET'
    },
    requestTitle: {
        NOTIFICATION: 'notification',
        FETCH_USER_BEHAVIOUR: 'fetch user behaviour'
    },
    baseConfigForRequestRetry: {
        retryDelay: 200,
        json: true
    }
};
