'use strict';

const chai = require('chai');
const expect = chai.expect;
const sinon = require('sinon');
chai.use(require('sinon-chai'));
const proxyquire = require('proxyquire').noCallThru();
const config = require('config');
const utils = require('../utils');

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
    runEngine,
    constructNotificationPayload,
    notifyAdminsOfNewlyFlaggedUser,
    constructPayloadForUserFlagTable,
    insertUserFlagIntoTable
} = fraud_detector;
const {
    accuracyStates,
    httpMethods,
    notificationTypes,
    delayForHttpRequestRetries,
    requestTitle
} = require('../constants');

const { EMAIL_TYPE } = notificationTypes;
const { POST } = httpMethods;
const { NOTIFICATION } = requestTitle;
const CONTACTS_TO_BE_NOTIFIED = config.get('contactsToBeNotified');

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
const sampleFacts = {
    userAccountInfo: sampleUserAccountInfo,
    lastDeposit: 10000,
    depositsLargerThanBaseIn6months: 4
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

    // TODO: entry point => implement and test 'retrieve facts from user-behaviour'
    // then proceed to run engine and send notification or not

    // test verbose mode
});
