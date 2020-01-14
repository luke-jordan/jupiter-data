'use strict';

const chai = require('chai');
const expect = chai.expect;
const sinon = require('sinon');
chai.use(require('sinon-chai'));
const proxyquire = require('proxyquire').noCallThru();
const uuid = require('uuid/v4');

const requestGetStub = sinon.stub();

const resetStubs = () => {
    requestGetStub.reset();
};

const sampleMessageId = uuid();
class MockPubSubClass {
    // eslint-disable-next-line class-methods-use-this
    topic () {
        return {
            publish: () => sampleMessageId
        };
    }
}

const MockGoogleCloud = {
    PubSub: MockPubSubClass
};

const snsToPubSubFunctions = proxyquire('../index', {
    '@google-cloud/pubsub': MockGoogleCloud
});

const {
    receiveNotification
} = snsToPubSubFunctions;

const { httpMethods } = require('../constants');
const {
    POST
} = httpMethods;

const parsedPayloadMessageFromSNS = {
    'eventType': 'SAVING_PAYMENT_SUCCESSFUL',
    'userId': uuid(),
    'timestamp': '123456',
    'context': {
        'accountId': uuid(),
        'firstSave': true,
        'saveCount': 1,
        'savedAmount': '40000000000::HUNDREDTH_CENT::ZAR',
        'timeInMillis': '2019-11-08T14:22:28.000Z',
        'transactionId': uuid()
    }
};

const samplePayloadMessageFromSNS = JSON.stringify(parsedPayloadMessageFromSNS);

describe('Amazon SNS to Google Pub/Sub Function', () => {
    beforeEach(() => {
        resetStubs();
    });

    it(`should only accept ${POST} http requests`, async () => {
        const req = {
            method: uuid(),
            body: samplePayloadMessageFromSNS
        };
        const endResponseStub = sinon.stub();
        const res = {
            status: () => ({
                end: endResponseStub
            })
        };

        const result = await receiveNotification(req, res);
        expect(result).to.be.undefined;
        expect(endResponseStub).to.have.been.calledOnce;
        expect(res.status().end.firstCall.args[0]).to.equal(`Only ${POST} http method accepted`);
    });

    it(`all valid SNS requests should have 'x-amz-sns-message-type' header`, async () => {
        requestGetStub.returns(null);
        const req = {
            method: POST,
            body: samplePayloadMessageFromSNS,
            get: requestGetStub
        };
        const endResponseStub = sinon.stub();
        const res = {
            status: () => ({
                end: endResponseStub
            })
        };

        const result = await receiveNotification(req, res);
        expect(result).to.be.undefined;
        expect(endResponseStub).to.have.been.calledOnce;
        expect(res.status().end.firstCall.args[0]).to.equal(`Invalid SNS message => at amz header`);
    });

    it(`should publish message to pubsub successfully`, async () => {
        requestGetStub.returns(uuid());
        const req = {
            method: POST,
            body: samplePayloadMessageFromSNS,
            get: requestGetStub
        };
        const endResponseStub = sinon.stub();
        const res = {
            status: () => ({
                end: endResponseStub
            })
        };

        const result = await receiveNotification(req, res);
        expect(result).to.be.undefined;
        expect(endResponseStub).to.have.been.calledOnce;
        expect(res.status().end.firstCall.args[0]).to.equal(sampleMessageId);
    });
});
