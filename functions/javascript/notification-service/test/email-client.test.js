'use strict';

const chai = require('chai');
const expect = chai.expect;
const sinon = require('sinon');
const config = require('config');
const shortid = require('shortid');
const proxyquire = require('proxyquire').noCallThru();
chai.use(require('sinon-chai'));

const transporterSendMailStub = sinon.stub();
const MockNodemailer = {
    createTransport: () => ({
        'sendMail': transporterSendMailStub
    })
};

const emailClientService = proxyquire('../email-client', {
    'nodemailer': MockNodemailer
});

const sampleMessage = 'Winter is coming';
const sampleEmail = ['johnsnow@gmail.com'];
const sampleRequestId = shortid.generate();
const {
    from,
    subject
} = config.get('mailFormat');

const ENVIRONMENT = config.get('environment');

const resetStubs = () => {
    transporterSendMailStub.reset();
};

describe('Email Client', () => {
    beforeEach(() => {
        resetStubs();
    });

    it('should send an email successfully', async () => {
        const params = {
            from,
            to: sampleEmail,
            subject: `${ENVIRONMENT} - ${subject}`,
            text: sampleMessage,
            html: sampleMessage
        };
        transporterSendMailStub.withArgs().resolves(params);
        const result = await emailClientService.sendEmail(sampleEmail, sampleMessage, sampleRequestId);
        expect(result).to.be.undefined;
        expect(transporterSendMailStub).to.have.been.calledWith(params);
    });

    it('should handle errors gracefully when sending an email', async () => {
        const params = {
            from,
            to: sampleEmail,
            subject: `${ENVIRONMENT} - ${subject}`,
            text: sampleMessage,
            html: sampleMessage
        };
        transporterSendMailStub.withArgs().rejects('Error while sending email');
        const result = await emailClientService.sendEmail(sampleEmail, sampleMessage, sampleRequestId);
        expect(result).to.be.undefined;
        expect(transporterSendMailStub).to.have.been.calledWith(params);
    });
});
