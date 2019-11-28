'use strict';

const chai = require('chai');
const expect = chai.expect;
const sinon = require('sinon');
chai.use(require('sinon-chai'));
const uuid = require('uuid/v4');

const constants = require('../constants');

const shortid = require('shortid');
const emailClientService = require('../email-client');
const notificationService = require('../notification-service');

const {
    notificationTypes,
    httpMethods
} = constants;
const { EMAIL_TYPE } = notificationTypes;
const { POST } = httpMethods;

const {
    sendEmailNotifications,
    sendMessageBasedOnType,
    sendMessageToContacts,
    missingParameterInReceivedPayload,
    handleSendNotificationRequest
} = notificationService;

const sendEmailStub = sinon.stub(emailClientService, 'sendEmail');
const sendMessageBasedOnTypeStub = sinon.stub(notificationService, 'sendMessageBasedOnType');

const resetStubs = () => {
    sendEmailStub.reset();
    sendMessageBasedOnTypeStub.reset();
};


const sampleMessage = 'Winter is coming';
const sampleContacts = ['johnsnow@gmail.com', 'tyrion@gmail.com'];
const sampleRequestId = shortid.generate();

const samplePayload = {
    contacts: sampleContacts,
    notificationType: EMAIL_TYPE,
    message: sampleMessage
};

const sampleOptions = {
    reqId: sampleRequestId
};

describe('Notification Service', () => {
    beforeEach(() => {
        resetStubs();
    });

    it('should send email notifications to all contacts in payload', async () => {
        sendEmailStub.resolves();
        const result = await sendEmailNotifications(samplePayload, sampleOptions);
        expect(result).to.be.undefined;
        expect(sendEmailStub).to.have.callCount(samplePayload.contacts.length);
    });

    it(`should send a message when type 'email' is specified`, async () => {
        sendEmailStub.resolves();
        const result = await sendMessageBasedOnType(samplePayload, sampleOptions);
        expect(result).to.be.undefined;
        expect(sendEmailStub).to.have.been.called;
    });

    it(`should throw an error when 'notificationType' is not supported`, async () => {
        const payload = { ...samplePayload, notificationType: uuid() };
        try {
            await sendMessageBasedOnType(payload, sampleOptions);
        } catch(error) {
            expect(error.message).equal('Notification Type not supported at the moment');
        }
    });

    // sendMessageToContacts
    // it(`should handle sending message to contacts`, async () => {
    //     sendMessageBasedOnTypeStub.withArgs(samplePayload, sampleOptions).returns();
    //     const result = await sendMessageToContacts(samplePayload, sampleRequestId);
    //     expect(result).to.be.undefined;
    //     expect(sendMessageBasedOnTypeStub).to.have.been.calledWith(samplePayload, sampleOptions);
    // });

    // missingParameterInReceivedPayload
    it(`should return 'false' as all required parameter are present`, async () => {
        let result = await missingParameterInReceivedPayload(samplePayload);
        expect(result).to.exist;
        expect(result).to.equal(false);
    });

    it(`should return 'true' when 'message' is missing from parameters`, async () => {
        const payloadWithMissingParameter = { ... samplePayload, message: null };
        const result = await missingParameterInReceivedPayload(payloadWithMissingParameter);
        expect(result).to.exist;
        expect(result).to.equal(true);
    });

    it(`should return 'true' when 'notificationType' is missing from parameters`, async () => {
        const payloadWithMissingParameter = { ... samplePayload, notificationType: null };
        const result = await missingParameterInReceivedPayload(payloadWithMissingParameter);
        expect(result).to.exist;
        expect(result).to.equal(true);
    });

    it(`should return 'true' when 'contacts' is missing from parameters`, async () => {
        const payloadWithMissingParameter = { ... samplePayload, contacts: null };
        const result = await missingParameterInReceivedPayload(payloadWithMissingParameter);
        expect(result).to.exist;
        expect(result).to.equal(true);
    });

    it(`should return 'true' when 'contacts' is an empty array in received parameters`, async () => {
        const payloadWithMissingParameter = { ... samplePayload, contacts: [] };
        const result = await missingParameterInReceivedPayload(payloadWithMissingParameter);
        expect(result).to.exist;
        expect(result).to.equal(true);
    });

    it(`should handle send notification requests successfully`, async () => {
        const req = {
            method: POST,
            body: { ...samplePayload },
        };
        const jsonResponseStub = sinon.stub();
        const res = {
            status: () => ({
                json: jsonResponseStub
            })
        };

        const result = await handleSendNotificationRequest(req, res);
        expect(result).to.be.undefined;
        expect(jsonResponseStub).to.have.been.calledOnce;
        expect(res.status().json.firstCall.args[0]).to.equal('Successfully sent notification request');
    });

    it(`handle send notification requests only accepts http method: ${POST}`, async () => {
        const req = {
            method: uuid(),
            body: { ...samplePayload },
        };
        const endResponseStub = sinon.stub();
        const res = {
            status: () => ({
                end: endResponseStub,
            })
        };

        const result = await handleSendNotificationRequest(req, res);
        expect(result).to.be.undefined;
        expect(endResponseStub).to.have.been.calledOnce;
        expect(res.status().end.firstCall.args[0]).to.equal(`only ${POST} http method accepted`);
    });

    it(`handle send notification requests checks for missing parameters in payload`, async () => {
        const req = {
            method: POST,
            body:  { ... samplePayload, message: null }
        };
        const endResponseStub = sinon.stub();
        const res = {
            status: () => ({
                end: endResponseStub,
            })
        };

        const result = await handleSendNotificationRequest(req, res);
        expect(result).to.be.undefined;
        expect(endResponseStub).to.have.been.calledOnce;
        expect(res.status().end.firstCall.args[0]).to.equal('invalid payload => \'notificationType\', \'contacts\' and \'message\' are required');
    });

    it(`send failure response when an error is thrown during the request`, async () => {
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

        const result = await handleSendNotificationRequest(req, res);
        expect(result).to.be.undefined;
        expect(endResponseStub).to.have.been.calledOnce;
        expect(res.status().end.firstCall.args[0]).to.equal('Unable to send notification request');
    });

    // test email client
});