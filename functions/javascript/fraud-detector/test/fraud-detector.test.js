'use strict';

const chai = require('chai');
const expect = chai.expect;
const sinon = require('sinon');
chai.use(require('sinon-chai'));
const proxyquire = require('proxyquire').noCallThru();
const config = require('config');

const bigQueryTableInsertStub = sinon.stub();
const requestRetryStub = sinon.stub();
const addRuleToRulesEngineStub = sinon.stub();
const engineRunStub = sinon.stub();


const resetStubs = () => {
    bigQueryTableInsertStub.reset();
    requestRetryStub.reset();
    addRuleToRulesEngineStub.reset();
    engineRunStub.reset();
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
    'json-rules-engine': MockRulesEngine
});
const {
    logFraudulentUserAndNotifyAdmins,
    runEngine
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

const sampleEvent = {
    params: {
        reasonForFlaggingUser: `User has deposited 50,000 rands 3 or more times in the last 6 months`
    }
};

const sampleNotificationPayload = {
    notificationType: EMAIL_TYPE,
    contacts: CONTACTS_TO_BE_NOTIFIED,
    message: `User has been flagged`
};

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

describe('Fraud Detector', () => {
    beforeEach(() => {
        resetStubs();
    });

    it('should log fraudulent user and notify admins process result of rules engine', async () => {
        bigQueryTableInsertStub.resolves();
        requestRetryStub.resolves();
        
        const result = await logFraudulentUserAndNotifyAdmins(sampleEvent, sampleUserAccountInfo);
        expect(result).to.be.undefined;
        expect(bigQueryTableInsertStub).to.have.been.calledOnce;
        expect(requestRetryStub).to.have.been.calledOnce;
        sinon.assert.callOrder(bigQueryTableInsertStub, requestRetryStub);
    });

    // it('should run the rules engine successfully', async () => {
    //     // addRuleToRulesEngineStub.resolves();
    //     // engineRunStub.resolves(sampleEvents);
    //
    //     bigQueryTableInsertStub.resolves();
    //     requestRetryStub.resolves();
    //
    //     const result = await runEngine(sampleFacts, sampleRules);
    //     expect(result).to.be.undefined;
    //
    //     // expect(addRuleToRulesEngineStub).to.have.been.calledTwice();
    //     // expect(engineRunStub).to.have.been.calledOnce();
    //
    //     expect(bigQueryTableInsertStub).to.have.been.calledOnce;
    //     expect(requestRetryStub).to.have.been.calledOnce;
    //
    //     // sinon.assert.callOrder(
    //     //     addRuleToRulesEngineStub,
    //     //     engineRunStub,
    //     //     bigQueryTableInsertStub,
    //     //     requestRetryStub
    //     // );
    // });


    // test logFraudulentUserFlag

    // run engine

    // retrieve facts from user-behaviour
    // entry point =>
    // then proceed to run engine and send notification or not

    // test verbose mode

});

// utils

//