'use strict';

const chai = require('chai');
const expect = chai.expect;
const sinon = require('sinon');
chai.use(require('sinon-chai'));
const proxyquire = require('proxyquire').noCallThru();
const config = require('config');
const utils = require('../utils');
const uuid = require('uuid/v4');

const bigQueryTableInsertStub = sinon.stub();
const requestRetryStub = sinon.stub();
const addRuleToRulesEngineStub = sinon.stub();
const engineRunStub = sinon.stub();
const timestampStub = sinon.stub(utils, 'createTimestampForSQLDatabase');

const resetStubs = () => {
    bigQueryTableInsertStub.reset();
    requestRetryStub.reset();
    addRuleToRulesEngineStub.reset();
    engineRunStub.reset();
    timestampStub.reset();
};

class MockBigQueryClass {
    constructor() {}

    dataset() {
        return {
            table: () => ({
                insert: bigQueryTableInsertStub
            })
        }
    }
}

const MockGoogleCloud = {
    BigQuery: MockBigQueryClass
};

class MockRulesEngineClass {
    constructor(){}

    addRule() {
        return addRuleToRulesEngineStub;
    }

    run() {
        return Promise.resolve(engineRunStub);
        // return new Promise((resolve) => resolve(engineRunStub));
    }
}

const MockRulesEngine = {
    Engine: MockRulesEngineClass
};

const fraud_detector = proxyquire('../fraud-detector', {
    '@google-cloud/bigquery': MockGoogleCloud,
    'requestretry': requestRetryStub,
    // 'json-rules-engine': MockRulesEngine
});
const {
    logFraudulentUserAndNotifyAdmins,
    extractReasonForFlaggingUserFromEvent,
    logFraudulentUserFlag,
    constructNotificationPayload,
    notifyAdminsOfNewlyFlaggedUser,
    constructPayloadForUserFlagTable,
    insertUserFlagIntoTable,
    fetchFactsAboutUserAndRunEngine,
    sendNotificationForVerboseMode
} = fraud_detector;
const {
    accuracyStates,
    httpMethods,
    notificationTypes,
    baseConfigForRequestRetry,
    requestTitle
} = require('../constants');

const { EMAIL_TYPE } = notificationTypes;
const {
    POST,
    GET
} = httpMethods;
const { NOTIFICATION } = requestTitle;
const CONTACTS_TO_BE_NOTIFIED = config.get('contactsToBeNotified');
const serviceUrls = config.get('serviceUrls');
const {
    NOTIFICATION_SERVICE_URL,
    USER_BEHAVIOUR_URL
}= serviceUrls;

const sampleUserAccountInfo = {
    userId: "1a",
    accountId: "3b43",
};
const sampleReasonForFlaggingUser = `User has deposited 50,000 rands 3 or more times in the last 6 months`;
const sampleEvent = {
    params: {
        reasonForFlaggingUser: sampleReasonForFlaggingUser
    }
};
const sampleNotificationPayload = {
    notificationType: EMAIL_TYPE,
    contacts: CONTACTS_TO_BE_NOTIFIED,
    message: `User has been flagged`
};
const sampleTimestamp = '2019-11-30 13:34:32';
const sampleRule1 = {
    conditions: {
        any: [
            {
                fact: 'lastDeposit',
                operator: 'greaterThan',
                value: 100000
            }
        ]
    },
    event: { // define the event to fire when the conditions evaluate truthy
        type: 'flaggedAsFraudulent',
        params: {
            reasonForFlaggingUser: `User's last deposit was greater than 100,000 rands`
        }
    }
};
const sampleRule2 = {
    conditions: {
        any: [
            {
                fact: 'depositsLargerThanBaseIn6months',
                operator: 'greaterThanInclusive',
                value: 3
            }
        ]
    },
    event: { // define the event to fire when the conditions evaluate truthy
        type: 'flaggedAsFraudulent',
        params: {
            reasonForFlaggingUser: `User has deposited 50,000 rands 3 or more times in the last 6 months`
        }
    }
};
const sampleRules = [sampleRule1, sampleRule2];
const sampleFactsFromUserBehaviour = {
    userAccountInfo: sampleUserAccountInfo,
    countOfDepositsGreaterThanHundredThousand: 1,
    countOfDepositsGreaterThanBenchmarkWithinSixMonthPeriod: 4
};
const sampleEvents = {
    events: [sampleEvent, sampleEvent]
};

const sampleRow = [
    {
        user_id: sampleUserAccountInfo.userId,
        account_id: sampleUserAccountInfo.accountId,
        reason: sampleReasonForFlaggingUser,
        accuracy: accuracyStates.pending,
        created_at: sampleTimestamp,
        updated_at: sampleTimestamp
    }
];
const samplePayloadFromFetchFactsTrigger = {
  userId: 'bkc-3a'  
};

describe('Utils for Fraud Detector', () => {
    it('should create timestamp for database successfully', async () => {
        timestampStub.callThrough();
        const result = await utils.createTimestampForSQLDatabase();
        expect(result).to.exist;
        expect(result).to.be.a('string');
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

    it(`should construct notification payload correctly`, async () => {
        const expectedResult = {
            notificationType: EMAIL_TYPE,
            contacts: CONTACTS_TO_BE_NOTIFIED,
            message: `User: ${sampleUserAccountInfo.userId} with account: ${sampleUserAccountInfo.accountId} has been flagged as fraudulent. Reason for flagging User: ${sampleReasonForFlaggingUser}`
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

    it('should log fraudulent user and notify admins', async () => {
        bigQueryTableInsertStub.resolves();
        requestRetryStub.resolves();

        const result = await logFraudulentUserAndNotifyAdmins(sampleEvent, sampleUserAccountInfo);
        expect(result).to.be.undefined;
        expect(bigQueryTableInsertStub).to.have.been.calledOnce;
        expect(requestRetryStub).to.have.been.calledOnce;
        sinon.assert.callOrder(bigQueryTableInsertStub, requestRetryStub);
    });

    it(`shoule extract 'reason' for flagging user from event`, async () => {
        const result = await extractReasonForFlaggingUserFromEvent(sampleEvent);
        expect(result).to.exist;
        expect(result).to.equal(sampleReasonForFlaggingUser);
    });

    it(`should insert user flag into table`, async () => {
        timestampStub.returns(sampleTimestamp);

        bigQueryTableInsertStub.resolves();
        const result = await insertUserFlagIntoTable(sampleRow);
        expect(result).to.be.undefined;
        expect(bigQueryTableInsertStub).to.have.been.calledOnce;
    });

    it(`should construct payload for user flag table successfully`, async () => {
        timestampStub.returns(sampleTimestamp);

        const result = await constructPayloadForUserFlagTable(sampleUserAccountInfo, sampleReasonForFlaggingUser);
        expect(result).to.exist;
        expect(result).to.deep.equal(sampleRow);
    });

    it(`should 'fetch facts about user and run engine' successfully`, async () => {
        requestRetryStub.onFirstCall().resolves();
        requestRetryStub.onSecondCall().resolves(sampleFactsFromUserBehaviour);

        const payload = {
            message: 'Just so you know, fraud detector ran',
            contacts: CONTACTS_TO_BE_NOTIFIED,
            notificationType: EMAIL_TYPE
        };
        const extraConfigForVerbose = {
            url: `${NOTIFICATION_SERVICE_URL}`,
            method: POST,
            body: payload,
        };

        const req = {
            method: POST,
            body: { ...samplePayloadFromFetchFactsTrigger },
        };
        const jsonResponseStub = sinon.stub();
        const res = {
            status: () => ({
                json: jsonResponseStub
            })
        };

        const result = await fetchFactsAboutUserAndRunEngine(req, res);
        expect(result).to.be.undefined;
        expect(requestRetryStub).to.have.been.calledTwice;
        expect(requestRetryStub).to.have.been.calledWith({
            ...baseConfigForRequestRetry,
            ...extraConfigForVerbose
        });
    });

    it(`should 'fetch facts about user and run engine' only accepts http method: ${POST}`, async () => {
        const req = {
            method: uuid(),
            body: { ...samplePayloadFromFetchFactsTrigger },
        };
        const endResponseStub = sinon.stub();
        const res = {
            status: () => ({
                end: endResponseStub,
            })
        };

        const result = await fetchFactsAboutUserAndRunEngine(req, res);
        expect(result).to.be.undefined;
        expect(endResponseStub).to.have.been.calledOnce;
        expect(res.status().end.firstCall.args[0]).to.equal(`only ${POST} http method accepted`);
    });

    it(`should 'fetch facts about user and run engine' checks for missing parameters in payload`, async () => {
        const req = {
            method: POST,
            body:  { userId: null }
        };
        const endResponseStub = sinon.stub();
        const res = {
            status: () => ({
                end: endResponseStub,
            })
        };

        const result = await fetchFactsAboutUserAndRunEngine(req, res);
        expect(result).to.be.undefined;
        expect(endResponseStub).to.have.been.calledOnce;
        expect(res.status().end.firstCall.args[0]).to.equal('invalid payload => \'userId\' is required');
    });

    it(`should send failure response when an error is thrown during the request`, async () => {
        const req = {
            method: POST,
            body:  null
        };
        const endResponseStub = sinon.stub();
        const res = {
            status: () => ({
                end: endResponseStub,
            })
        };

        const result = await fetchFactsAboutUserAndRunEngine(req, res);
        expect(result).to.be.undefined;
        expect(endResponseStub).to.have.been.calledOnce;
        expect(res.status().end.firstCall.args[0]).to.equal('Unable to check for fraudulent user');
    });

    // test verbose mode
    it(`should send notification for verbose mode when 'verbose mode' is ON`, async () => {
        const payload = {
            message: 'Just so you know, fraud detector ran',
            contacts: CONTACTS_TO_BE_NOTIFIED,
            notificationType: EMAIL_TYPE
        };
        const extraConfig = {
            url: `${NOTIFICATION_SERVICE_URL}`,
            method: POST,
            body: payload,
        };
        requestRetryStub.resolves();
        const result = await sendNotificationForVerboseMode();
        expect(result).to.be.undefined;
        expect(requestRetryStub).to.have.been.calledWith({
            ...baseConfigForRequestRetry,
            ...extraConfig
        });
    });
});
