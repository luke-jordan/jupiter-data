'use strict';

const chai = require('chai');
const expect = chai.expect;
const sinon = require('sinon');
chai.use(require('sinon-chai'));
const proxyquire = require('proxyquire').noCallThru();
const uuid = require('uuid/v4');

const requestGetStub = sinon.stub();
const httpsGetStub = sinon.stub();
const httpsOnStub = sinon.stub();

const resetStubs = () => {
    requestGetStub.reset();
    httpsGetStub.reset();
    httpsOnStub.reset();
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

const MockHttpsLibrary = {
    get: httpsGetStub,
    on: httpsOnStub
};

const MockGoogleCloud = {
    PubSub: MockPubSubClass
};

const snsToPubSubFunctions = proxyquire('../index', {
    '@google-cloud/pubsub': MockGoogleCloud,
    'https': MockHttpsLibrary
});

const {
    receiveNotification
} = snsToPubSubFunctions;

const { httpMethods, SUBSCRIPTION_CONFIRMATION } = require('../constants');
const {
    POST
} = httpMethods;

const rawPayloadForSubscription = {
    'Type': SUBSCRIPTION_CONFIRMATION,
    'userId': 'a100',
    'timestamp': '123456',
    'MessageId': uuid(),
    'Subject': '4 Hello World',
    'Token': uuid(),
    'TopicArn': 'arn:aws:sns:us-east-1:test',
    'Message': '3 This is beautiful',
    'SubscribeURL': 'https://sns.us-east-1.amazonaws.com/?Action=ConfirmSubscription&TopicArn=arn:aws:sns:us-east-1:455943420663:dynamodb&Token=2336412f37fb687f5d51e6e241dbca538ac3572842f0dbaa070ed41be5b11f8e48fd6eece193a1f034a7c38cd905b50820fffe51b21ad28ba987245303080418a457539ce844dda369c07c24b418ce84c45de281c082d6d20e8691f9c9d7984902b1be635917f91c3cc05258fd7864a8',
    'Timestamp': '2019-10-08T15:23:57.229Z',
    'SignatureVersion': '1',
    'Signature': uuid(),
    'SigningCertURL': 'https://sns.us-east-1.amazonaws.com/SimpleNotificationService-6aad65c2f9911b05cd53efda11f913f9.pem'
};
const samplePayloadForSubscription = JSON.stringify(rawPayloadForSubscription);

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

    // it(`should handle errors that occur while publishing message to pubsub`, async () => {
    //
    // });

    // it(`should handle request to 'confirm subscriptions' successfully`, async () => {
    //     requestGetStub.returns("");
    //     httpsGetStub.returns("");
    //     httpsOnStub.returns("");
    //     const req = {
    //         method: POST,
    //         body: samplePayloadForSubscription,
    //         get: requestGetStub
    //     };
    //     const endResponseStub = sinon.stub();
    //     const res = {
    //         status: () => ({
    //             end: endResponseStub
    //         })
    //     };
    //
    //     const result = await receiveNotification(req, res);
    //     expect(result).to.be.undefined;
    //     expect(endResponseStub).to.have.been.calledOnce;
    //     expect(res.status().end.firstCall.args[0]).to.equal(`Invalid SNS message => at amz header`);
    // });

});
