'use strict';

const chai = require('chai');
const expect = chai.expect;
const sinon = require('sinon');
chai.use(require('sinon-chai'));
const proxyquire = require('proxyquire').noCallThru();
const config = require('config');
const utils = require('../utils');
const uuid = require('uuid/v4');
const httpStatus = require('http-status');

const bigQueryTableInsertStub = sinon.stub();
const bigQueryFetchStub = sinon.stub();
const requestRetryStub = sinon.stub();
const timestampStub = sinon.stub(utils, 'generateCurrentTimeInMilliseconds');
const BIG_QUERY_DATASET_LOCATION = config.get('BIG_QUERY_DATASET_LOCATION');
const GOOGLE_PROJECT_ID = config.get('GOOGLE_PROJECT_ID');
const DATASET_ID = config.get('BIG_QUERY_DATASET_ID');
const TABLE_ID = config.get('BIG_QUERY_TABLE_ID');

const resetStubs = () => {
    bigQueryTableInsertStub.reset();
    bigQueryFetchStub.reset();
    requestRetryStub.reset();
    timestampStub.reset();
};

const timestamp = 1554249600000;
const sampleFlagTime = timestamp;
const sampleRuleLabel = 'single_very_large_saving_event';
// eslint-disable-next-line camelcase
const sampleRuleListWithLatestFlagTime = [[{ rule_label: sampleRuleLabel, latest_flag_time: sampleFlagTime }]];
const userId = '1a';
const accountId = '3b43';
const sampleUserAccountInfo = {
    userId,
    accountId
};
const sampleCreatedAtTime = 1577138301793;
const sampleUpdatedAtTime = 1577138301793;
const sampleRuleLabel1 = 'single_very_large_saving_event';
const sampleRuleLabel2 = 'latest_saving_event_greater_than_six_months_average';
const sampleAccuracy = 'PENDING_CONFIRMATION';
const sampleResponseForFetchUserDetailFromFlagTable = [
    {
        'user_id': userId,
        'account_id': accountId,
        'rule_label': sampleRuleLabel1,
        'reason': 'User has a deposit greater than 100,000 rands',
        'accuracy': sampleAccuracy,
        'created_at': sampleCreatedAtTime,
        'updated_at': sampleUpdatedAtTime
    },
    {
        'user_id': userId,
        'account_id': accountId,
        'rule_label': sampleRuleLabel2,
        'reason': `User's latest inward transfer > 10x past 6 month average transfer`,
        'accuracy': sampleAccuracy,
        'created_at': sampleCreatedAtTime,
        'updated_at': sampleUpdatedAtTime
    }
];
const optionsForFetchUserDetail = {
    query: `
            SELECT *
            FROM \`${GOOGLE_PROJECT_ID}.${DATASET_ID}.${TABLE_ID}\`
            where \`user_id\` = @userId and \`account_id\` = @accountId
        `,
    location: BIG_QUERY_DATASET_LOCATION,
    params: sampleUserAccountInfo
};

class MockBigQueryClass {
    // eslint-disable-next-line class-methods-use-this
    dataset () {
        return {
            table: () => ({
                insert: bigQueryTableInsertStub
            })
        };
    }

    // eslint-disable-next-line class-methods-use-this
    query (options) {
        if (options.query === optionsForFetchUserDetail.query) {
            return [sampleResponseForFetchUserDetailFromFlagTable];
        }

        return sampleRuleListWithLatestFlagTime;
    }
}

const MockGoogleCloud = {
    BigQuery: MockBigQueryClass
};

const fraudDetector = proxyquire('../fraud-detector', {
    '@google-cloud/bigquery': MockGoogleCloud,
    'requestretry': requestRetryStub
});
const {
    logFraudulentUserAndNotifyAdmins,
    logFraudulentUserFlag,
    constructNotificationPayload,
    notifyAdminsOfNewlyFlaggedUser,
    constructPayloadForUserFlagTable,
    insertUserFlagIntoTable,
    logUserNotFlagged,
    fetchFactsAboutUserAndRunEngine,
    sendNotificationOfResultToAdmins,
    isRuleSafeOrIsExperimentalModeOn,
    fetchUserDetailsFromFlagTable,
    constructOptionsForUserDetailsFetchFromFlagTable
} = fraudDetector;
const {
    accuracyStates,
    httpMethods,
    notificationTypes,
    baseConfigForRequestRetry
} = require('../constants');

const indexNotExist = -1;

const { EMAIL_TYPE } = notificationTypes;
const {
    POST
} = httpMethods;
const CONTACTS_TO_BE_NOTIFIED = config.get('contactsToBeNotified');
const serviceUrls = config.get('serviceUrls');
const EMAIL_SUBJECT_FOR_ADMINS = config.get('emailSubjectForAdmins');
const {
    NOTIFICATION_SERVICE_URL,
    AUTH_SERVICE_URL,
    FETCH_USER_BEHAVIOUR_URL
} = serviceUrls;

const samplePayloadForFetchUserDetail = sampleUserAccountInfo;

const formattedRulesWithLatestFlagTime = {
    [sampleRuleLabel]: sampleFlagTime
};
const sampleReasonForFlaggingUser = `User has saving_event 50,000 rands 3 or more times in the last 6 months`;
const sampleEvent = {
    params: {
        ruleLabel: sampleRuleLabel,
        reasonForFlaggingUser: sampleReasonForFlaggingUser
    }
};

const sampleTimestamp = timestamp;
const sampleFactsFromUserBehaviour = {
    userAccountInfo: sampleUserAccountInfo,
    countOfSavingEventsGreaterThanHundredThousand: 1,
    countOfSavingEventsGreaterThanBenchmarkWithinSixMonthPeriod: 4
};

const sampleRow = [
    /*eslint-disable */
    /**
     * eslint disable is needed to turn off the eslint rule: 'camelcase'
     * The below values represent the columns of the user_flag tables and the column names are not in camelcase
     */
    {
        user_id: sampleUserAccountInfo.userId,
        account_id: sampleUserAccountInfo.accountId,
        rule_label: sampleRuleLabel,
        reason: sampleReasonForFlaggingUser,
        accuracy: accuracyStates.pending,
        created_at: sampleTimestamp,
        updated_at: sampleTimestamp
    }
    /* eslint-enable */
];

const samplePayloadFromFetchFactsTrigger = sampleUserAccountInfo;

const ruleWithExperimentalModeTrue = {
    experimental: true,
    conditions: {
        any: [
            {
                fact: 'countOfSavingEventsGreaterThanHundredThousand',
                operator: 'greaterThan',
                value: 0
            }
        ]
    },
    event: { // define the event to fire when the conditions evaluate truthy
        type: 'flaggedAsFraudulent',
        params: {
            reasonForFlaggingUser: `User has a saving_event greater than 100,000 rands`
        }
    }
};

const ruleWithExperimentalModeFalse = {
    experimental: false,
    conditions: {
        any: [
            {
                fact: 'countOfSavingEventsGreaterThanHundredThousand',
                operator: 'greaterThan',
                value: 0
            }
        ]
    },
    event: { // define the event to fire when the conditions evaluate truthy
        type: 'flaggedAsFraudulent',
        params: {
            reasonForFlaggingUser: `User has a saving_event greater than 100,000 rands`
        }
    }
};

const ruleWithoutExperimentalMode = {
    conditions: {
        any: [
            {
                fact: 'countOfSavingEventsGreaterThanHundredThousand',
                operator: 'greaterThan',
                value: 0
            }
        ]
    },
    event: { // define the event to fire when the conditions evaluate truthy
        type: 'flaggedAsFraudulent',
        params: {
            reasonForFlaggingUser: `User has a saving_event greater than 100,000 rands`
        }
    }
};

const sampleResponseFromAuthService = {
    userRole: 'SYSTEM_ADMIN'
};
const sampleRequestHeaders = { Authentication: 'Bearer xxx' };

describe('Utils for Fraud Detector', () => {
    it('should create timestamp for database successfully', async () => {
        timestampStub.callThrough();
        const result = await utils.generateCurrentTimeInMilliseconds();
        expect(result).to.exist;
        expect(result).to.be.a('number');
    });
});

describe('Fraud Detector', () => {
    beforeEach(() => {
        resetStubs();
    });

    it(`should log fraudulent user flag`, async () => {
        bigQueryTableInsertStub.resolves();

        const result = await logFraudulentUserFlag(sampleUserAccountInfo, sampleReasonForFlaggingUser);
        expect(result).to.be.undefined;
        expect(bigQueryTableInsertStub).to.have.been.calledOnce;

    });

    it(`should log user not flagged`, async () => {
        bigQueryTableInsertStub.resolves();

        const result = await logUserNotFlagged(sampleUserAccountInfo);
        expect(result).to.be.undefined;
        expect(bigQueryTableInsertStub).to.have.been.calledOnce;
    });

    it(`should construct notification payload correctly`, async () => {
        const expectedResult = {
            notificationType: EMAIL_TYPE,
            contacts: CONTACTS_TO_BE_NOTIFIED,
            message: `User: ${sampleUserAccountInfo.userId} with account: ${sampleUserAccountInfo.accountId} has been flagged as fraudulent. Reason for flagging User: ${sampleReasonForFlaggingUser}`,
            subject: EMAIL_SUBJECT_FOR_ADMINS
        };
        const result = await constructNotificationPayload(sampleUserAccountInfo, sampleReasonForFlaggingUser);
        expect(result).to.exist;
        expect(result).to.deep.equal(expectedResult);
    });

    it(`should notify admins of newly flagged user`, async () => {
        requestRetryStub.resolves();
        const result = await notifyAdminsOfNewlyFlaggedUser();
        expect(result).to.be.undefined;
        expect(requestRetryStub).to.have.been.calledOnce;
    });

    it(`should handle errors gracefully while 'notifying admins of newly flagged user'`, async () => {
        requestRetryStub.rejects();
        const result = await notifyAdminsOfNewlyFlaggedUser();
        expect(result).to.be.undefined;
        expect(requestRetryStub).to.have.been.calledOnce;
    });

    it('should log fraudulent user and notify admins', async () => {
        bigQueryTableInsertStub.resolves();
        requestRetryStub.resolves();

        const result = await logFraudulentUserAndNotifyAdmins(sampleEvent, sampleUserAccountInfo);
        expect(result).to.be.undefined;
        expect(bigQueryTableInsertStub).to.have.been.calledOnce;
        expect(requestRetryStub).to.have.been.calledOnce;
        sinon.assert.callOrder(bigQueryTableInsertStub, requestRetryStub);
    });

    it(`should insert user flag into table`, async () => {
        bigQueryTableInsertStub.resolves();
        const result = await insertUserFlagIntoTable(sampleRow);
        expect(result).to.be.undefined;
        expect(bigQueryTableInsertStub).to.have.been.calledOnce;
    });

    it(`should construct payload for user flag table successfully`, async () => {
        timestampStub.returns(sampleTimestamp);

        const result = await constructPayloadForUserFlagTable(
            sampleUserAccountInfo,
            sampleRuleLabel,
            sampleReasonForFlaggingUser
        );
        expect(result).to.exist;
        expect(result).to.deep.equal(sampleRow);
    });

    it(`should 'fetch facts about user and run engine' successfully`, async () => {
        requestRetryStub.onFirstCall().resolves({
            body: sampleResponseFromAuthService,
            statusCode: httpStatus.OK
        });
        requestRetryStub.onSecondCall().resolves({
            body: sampleFactsFromUserBehaviour,
            statusCode: httpStatus.OK
        });

        const extraConfigForFetchUserBehaviour = {
            url: `${FETCH_USER_BEHAVIOUR_URL}`,
            method: POST,
            body: {
                userId,
                accountId,
                ruleCutOffTimes: formattedRulesWithLatestFlagTime
            }
        };

        const req = {
            headers: sampleRequestHeaders,
            method: POST,
            body: { ...samplePayloadFromFetchFactsTrigger }
        };
        const endResponseStub = sinon.stub();
        const jsonResponseStub = sinon.stub();
        const res = {
            status: () => ({
                json: jsonResponseStub,
                end: endResponseStub
            })
        };

        const extraConfigForFetchUserDetailFromFlagTable = {
            url: `${AUTH_SERVICE_URL}`,
            method: POST,
            body: { token: req.headers['Authentication'].substring('Bearer '.length) }
        };

        const result = await fetchFactsAboutUserAndRunEngine(req, res);
        expect(result).to.be.undefined;
        expect(endResponseStub).to.have.been.calledOnce;
        expect(requestRetryStub).to.have.been.calledTwice;
        expect(requestRetryStub.firstCall.args[0]).to.deep.equal({
            ...baseConfigForRequestRetry,
            ...extraConfigForFetchUserDetailFromFlagTable
        });
        expect(requestRetryStub.secondCall.args[0]).to.deep.equal({
            ...baseConfigForRequestRetry,
            ...extraConfigForFetchUserBehaviour
        });
    });

    it(`should notify properly on failure -> 'fetch facts about user and run engine'`, async () => {
        requestRetryStub.onFirstCall().resolves({
            body: sampleResponseFromAuthService,
            statusCode: httpStatus.OK
        });
        requestRetryStub.onSecondCall().resolves({});

        const messageWithoutErrorStackValue = `Just so you know, fraud detector ran. Extra info: {"errorMessage":"Could not fetch facts from user behaviour","errorStack"`;
        const payloadWithoutMessage = {
            contacts: CONTACTS_TO_BE_NOTIFIED,
            notificationType: EMAIL_TYPE,
            subject: '[FAILED] => Fraud Detector just ran'
        };
        const extraConfigForVerboseWithoutMessage = {
            url: `${NOTIFICATION_SERVICE_URL}`,
            method: POST,
            body: payloadWithoutMessage
        };

        const extraConfigForFetchUserBehaviour = {
            url: `${FETCH_USER_BEHAVIOUR_URL}`,
            method: POST,
            body: {
                userId,
                accountId,
                ruleCutOffTimes: formattedRulesWithLatestFlagTime
            }
        };

        const req = {
            headers: sampleRequestHeaders,
            method: POST,
            body: { ...samplePayloadFromFetchFactsTrigger }
        };

        const extraConfigForFetchUserDetailFromFlagTable = {
            url: `${AUTH_SERVICE_URL}`,
            method: POST,
            body: { token: req.headers['Authentication'].substring('Bearer '.length) }
        };

        const endResponseStub = sinon.stub();
        const jsonResponseStub = sinon.stub();
        const res = {
            status: () => ({
                json: jsonResponseStub,
                end: endResponseStub
            })
        };

        const result = await fetchFactsAboutUserAndRunEngine(req, res);
        expect(result).to.be.undefined;
        expect(endResponseStub).to.have.been.calledOnce;
        expect(requestRetryStub).to.have.been.calledThrice;
        expect(requestRetryStub.firstCall.args[0]).to.deep.equal({
            ...baseConfigForRequestRetry,
            ...extraConfigForFetchUserDetailFromFlagTable
        });
        expect(requestRetryStub.secondCall.args[0]).to.deep.equal({
            ...baseConfigForRequestRetry,
            ...extraConfigForFetchUserBehaviour
        });

        const messageForSendNotificationResultToAdmins = requestRetryStub.thirdCall.args[0].body.message;
        const indexOfErrorStackInMessage = messageForSendNotificationResultToAdmins.indexOf(`errorStack`);

        if (indexOfErrorStackInMessage === indexNotExist) {
            throw new Error('errorStack must be in message body of payload');
        } else {
            // test message value in body or payload being sent as a notification
            const indexOfStringBeforeErrorStackValue = indexOfErrorStackInMessage + 'errorStack'.length + 1;
            expect(messageForSendNotificationResultToAdmins.substring(0, indexOfStringBeforeErrorStackValue)).to.equal(messageWithoutErrorStackValue);
            expect(messageForSendNotificationResultToAdmins.substring(indexOfStringBeforeErrorStackValue)).to.exist;

            // delete message property from body of the payload as message property might be unpredictable with an error stack embedded in the message
            Reflect.deleteProperty(requestRetryStub.thirdCall.args[0].body, 'message');
        }

        expect(requestRetryStub.thirdCall.args[0]).to.deep.equal({
            ...baseConfigForRequestRetry,
            ...extraConfigForVerboseWithoutMessage
        });

    });

    it(`should 'fetch facts about user and run engine' only accepts http method: ${POST}`, async () => {
        const req = {
            method: uuid(),
            body: { ...samplePayloadFromFetchFactsTrigger }
        };
        const endResponseStub = sinon.stub();
        const res = {
            status: () => ({
                end: endResponseStub
            })
        };

        const result = await fetchFactsAboutUserAndRunEngine(req, res);
        expect(result).to.be.undefined;
        expect(endResponseStub).to.have.been.calledTwice;
        expect(res.status().end.firstCall.args[0]).to.equal(`Received request to 'check for fraudulent user'`);
        expect(res.status().end.secondCall.args[0]).to.equal(`Only ${POST} http method accepted`);
    });

    it(`should 'fetch facts about user and run engine' checks for missing 'userId' parameter in payload`, async () => {
        const req = {
            headers: sampleRequestHeaders,
            method: POST,
            body: { userId: null, accountId: 'hdfl' }
        };
        requestRetryStub.onFirstCall().resolves({
            body: sampleResponseFromAuthService,
            statusCode: httpStatus.OK
        });
        const endResponseStub = sinon.stub();
        const res = {
            status: () => ({
                end: endResponseStub
            })
        };

        const extraConfigForFetchUserDetailFromFlagTable = {
            url: `${AUTH_SERVICE_URL}`,
            method: POST,
            body: { token: req.headers['Authentication'].substring('Bearer '.length) }
        };

        const result = await fetchFactsAboutUserAndRunEngine(req, res);
        expect(result).to.be.undefined;
        expect(endResponseStub).to.have.been.calledTwice;
        expect(requestRetryStub.firstCall.args[0]).to.deep.equal({
            ...baseConfigForRequestRetry,
            ...extraConfigForFetchUserDetailFromFlagTable
        });
        expect(res.status().end.firstCall.args[0]).to.equal(`Received request to 'check for fraudulent user'`);
        expect(res.status().end.secondCall.args[0]).to.equal('invalid payload => \'userId\' and \'accountId\' are required');
    });

    it(`should 'fetch facts about user and run engine' checks for missing 'accountId' parameter in payload`, async () => {
        const req = {
            headers: sampleRequestHeaders,
            method: POST,
            body: { userId: 'kfld', accountId: null }
        };
        requestRetryStub.onFirstCall().resolves({
            body: sampleResponseFromAuthService,
            statusCode: httpStatus.OK
        });
        const endResponseStub = sinon.stub();
        const res = {
            status: () => ({
                end: endResponseStub
            })
        };

        const extraConfigForFetchUserDetailFromFlagTable = {
            url: `${AUTH_SERVICE_URL}`,
            method: POST,
            body: { token: req.headers['Authentication'].substring('Bearer '.length) }
        };

        const result = await fetchFactsAboutUserAndRunEngine(req, res);
        expect(result).to.be.undefined;
        expect(requestRetryStub.firstCall.args[0]).to.deep.equal({
            ...baseConfigForRequestRetry,
            ...extraConfigForFetchUserDetailFromFlagTable
        });
        expect(endResponseStub).to.have.been.calledTwice;
        expect(res.status().end.firstCall.args[0]).to.equal(`Received request to 'check for fraudulent user'`);
        expect(res.status().end.secondCall.args[0]).to.equal('invalid payload => \'userId\' and \'accountId\' are required');
    });

    it(`should send failure response when the 'body' of the request does not exist`, async () => {
        const req = {
            headers: sampleRequestHeaders,
            method: POST,
            body: null
        };
        requestRetryStub.onFirstCall().resolves({
            body: sampleResponseFromAuthService,
            statusCode: httpStatus.OK
        });
        const endResponseStub = sinon.stub();
        const res = {
            status: () => ({
                end: endResponseStub
            })
        };

        const extraConfigForFetchUserDetailFromFlagTable = {
            url: `${AUTH_SERVICE_URL}`,
            method: POST,
            body: { token: req.headers['Authentication'].substring('Bearer '.length) }
        };

        const result = await fetchFactsAboutUserAndRunEngine(req, res);
        expect(result).to.be.undefined;
        expect(endResponseStub).to.have.been.calledTwice;
        expect(requestRetryStub.firstCall.args[0]).to.deep.equal({
            ...baseConfigForRequestRetry,
            ...extraConfigForFetchUserDetailFromFlagTable
        });
        expect(res.status().end.firstCall.args[0]).to.equal(`Received request to 'check for fraudulent user'`);
        expect(res.status().end.secondCall.args[0]).to.equal('Unable to check for fraudulent user');
    });

    it(`should exit gracefully when it cannot 'fetch facts from user behaviour service'`, async () => {
        const req = {
            headers: sampleRequestHeaders,
            method: POST,
            body: { ...samplePayloadFromFetchFactsTrigger }
        };
        const endResponseStub = sinon.stub();
        const res = {
            status: () => ({
                end: endResponseStub
            })
        };
        requestRetryStub.onFirstCall().resolves({
            body: sampleResponseFromAuthService,
            statusCode: httpStatus.OK
        });
        requestRetryStub.onSecondCall().rejects();
        requestRetryStub.onThirdCall().resolves();
        const extraConfigForFetchUserDetailFromFlagTable = {
            url: `${AUTH_SERVICE_URL}`,
            method: POST,
            body: { token: req.headers['Authentication'].substring('Bearer '.length) }
        };

        const result = await fetchFactsAboutUserAndRunEngine(req, res);
        expect(result).to.be.undefined;
        expect(endResponseStub).to.have.been.calledOnce;
        expect(requestRetryStub).to.have.been.calledThrice;
        expect(requestRetryStub.firstCall.args[0]).to.deep.equal({
            ...baseConfigForRequestRetry,
            ...extraConfigForFetchUserDetailFromFlagTable
        });
        expect(res.status().end.firstCall.args[0]).to.equal(`Received request to 'check for fraudulent user'`);
    });

    it(`should send notification for verbose mode when 'verbose mode' is ON`, async () => {
        const payload = {
            message: 'Just so you know, fraud detector ran',
            contacts: CONTACTS_TO_BE_NOTIFIED,
            notificationType: EMAIL_TYPE,
            subject: '[SUCCESS] => Fraud Detector just ran'
        };
        const extraConfig = {
            url: `${NOTIFICATION_SERVICE_URL}`,
            method: POST,
            body: payload
        };
        requestRetryStub.resolves();
        const requestStatus = {
            result: 'SUCCESS'
        };
        const result = await sendNotificationOfResultToAdmins(requestStatus);
        expect(result).to.be.undefined;
        expect(requestRetryStub).to.have.been.calledWith({
            ...baseConfigForRequestRetry,
            ...extraConfig
        });
    });

    it(`should handle errors gracefully when 'insert user flag into table' fails`, async () => {
        bigQueryTableInsertStub.rejects();
        const result = await insertUserFlagIntoTable(sampleRow);
        expect(result).to.be.undefined;
        expect(bigQueryTableInsertStub).to.have.been.calledOnce;
    });

    it(`should allow only 'non-experimental' rules when 'general experimental mode is off'`, async () => {
        const resultOfExperimentalModeTrue = await isRuleSafeOrIsExperimentalModeOn(ruleWithExperimentalModeTrue);
        expect(resultOfExperimentalModeTrue).to.equal(false);

        const resultOfExperimentalModeFalse = await isRuleSafeOrIsExperimentalModeOn(ruleWithExperimentalModeFalse);
        expect(resultOfExperimentalModeFalse).to.equal(true);

        const resultOfWithoutExperimentalMode = await isRuleSafeOrIsExperimentalModeOn(ruleWithoutExperimentalMode);
        expect(resultOfWithoutExperimentalMode).to.equal(true);
    });

    it(`should construct options to 'fetch user data from flag table' successfully`, async () => {
        const expectedQuery = `
            SELECT *
            FROM \`${GOOGLE_PROJECT_ID}.${DATASET_ID}.${TABLE_ID}\`
            where \`user_id\` = @userId and \`account_id\` = @accountId
        `;

        const expectedOptions = {
            query: expectedQuery,
            location: BIG_QUERY_DATASET_LOCATION,
            params: {
                userId,
                accountId
            }
        };

        const result = await constructOptionsForUserDetailsFetchFromFlagTable(sampleUserAccountInfo, expectedQuery);
        expect(result).to.exist;
        expect(result).to.deep.equal(expectedOptions);
    });

    it(`should 'fetch user data from flag table' successfully`, async () => {
        const req = {
            headers: sampleRequestHeaders,
            method: POST,
            body: { ...samplePayloadForFetchUserDetail}
        };
        requestRetryStub.onFirstCall().resolves({
            body: sampleResponseFromAuthService,
            statusCode: httpStatus.OK
        });
        const jsonResponseStub = sinon.stub();
        const res = {
            status: () => ({
                json: jsonResponseStub
            })
        };
        const extraConfigForFetchUserDetailFromFlagTable = {
            url: `${AUTH_SERVICE_URL}`,
            method: POST,
            body: { token: req.headers['Authentication'].substring('Bearer '.length) }
        };

        const result = await fetchUserDetailsFromFlagTable(req, res);
        expect(result).to.be.undefined;
        expect(jsonResponseStub).to.have.been.calledOnce;
        expect(requestRetryStub.firstCall.args[0]).to.deep.equal({
            ...baseConfigForRequestRetry,
            ...extraConfigForFetchUserDetailFromFlagTable
        });
        expect(res.status().json.firstCall.args[0]).to.deep.equal(sampleResponseForFetchUserDetailFromFlagTable);
    });

});
