'use strict';

const chai = require('chai');
const expect = chai.expect;
const sinon = require('sinon');
chai.use(require('sinon-chai'));
const proxyquire = require('proxyquire').noCallThru();
const uuid = require('uuid/v4');


const extractedSampleResponseFromBigQuery = [
    {
        'event_type': 'USER_ENTERED_SCREEN',
        'event_count': 7
    },
    {
        'event_type': 'PAYMENT_SUCCEEDED',
        'event_count': 5
    }
];
const rawSampleResponseFromBigQuery = [extractedSampleResponseFromBigQuery];

class MockBigQueryClass {
    // eslint-disable-next-line class-methods-use-this
    query () {
        return rawSampleResponseFromBigQuery;
    }
}

const MockGoogleCloud = {
    BigQuery: MockBigQueryClass
};

const fetchFromBigQueryFunctions = proxyquire('../index', {
    '@google-cloud/bigquery': MockGoogleCloud
});

const {
    fetchFromBigQuery,
    constructSQLQueryForAmplitudeFetch,
    convertDateTimeStringToMillisecondsInteger,
    constructOptionsForBigQueryFetch
} = fetchFromBigQueryFunctions;

const dotenv = require('dotenv');
dotenv.config();
const GOOGLE_PROJECT_ID = process.env.GOOGLE_PROJECT_ID;
const BIG_QUERY_DATASET_LOCATION = process.env.BIG_QUERY_DATASET_LOCATION;

const constants = require('../constants');
const {
    httpMethods,
    DATASET,
    TABLE,
    timeAtStartOfDay,
    timeAtEndOfDay,
    SOURCE_OF_EVENT
} = constants;
const {
    POST
} = httpMethods;

const startDateFilter = '2019-04-03';
const endDateFilter = '2019-04-06';
const dateConversionToMilliseconds = {
    [startDateFilter]: 1554249600000,
    [endDateFilter]: 1554595199000
};

const samplePayloadForFetchAmplitudeData = {
    'startDate': '2019-12-19',
    'endDate': '2019-12-19',
    'eventTypes': [
        'USER_ENTERED_SCREEN',
        'PAYMENT_SUCCEEDED'
    ]
};


describe('Fetch Amplitude Data from Big Query', () => {
    it(`should only accept ${POST} http requests`, async () => {
        const req = {
            method: uuid(),
            body: { ...samplePayloadForFetchAmplitudeData }
        };
        const endResponseStub = sinon.stub();
        const res = {
            status: () => ({
                end: endResponseStub
            })
        };

        const result = await fetchFromBigQuery(req, res);
        expect(result).to.be.undefined;
        expect(endResponseStub).to.have.been.calledOnce;
        expect(res.status().end.firstCall.args[0]).to.equal(`Only ${POST} http method accepted`);
    });

    it(`should check for missing 'startDate' parameter in payload`, async () => {
        const req = {
            method: POST,
            body: { ...samplePayloadForFetchAmplitudeData, startDate: null }
        };
        const endResponseStub = sinon.stub();
        const res = {
            status: () => ({
                end: endResponseStub
            })
        };

        const result = await fetchFromBigQuery(req, res);
        expect(result).to.be.undefined;
        expect(endResponseStub).to.have.been.calledOnce;
        expect(res.status().end.firstCall.args[0]).to.equal(`Invalid parameters => 'startDate', 'endDate' and 'eventTypes' are required`);
    });

    it(`should check for missing 'endDate' parameter in payload`, async () => {
        const req = {
            method: POST,
            body: { ...samplePayloadForFetchAmplitudeData, endDate: null }
        };
        const endResponseStub = sinon.stub();
        const res = {
            status: () => ({
                end: endResponseStub
            })
        };

        const result = await fetchFromBigQuery(req, res);
        expect(result).to.be.undefined;
        expect(endResponseStub).to.have.been.calledOnce;
        expect(res.status().end.firstCall.args[0]).to.equal(`Invalid parameters => 'startDate', 'endDate' and 'eventTypes' are required`);
    });
    
    it(`should check for missing 'eventTypes' parameter in payload`, async () => {
        const req = {
            method: POST,
            body: { ...samplePayloadForFetchAmplitudeData, eventTypes: null }
        };
        const endResponseStub = sinon.stub();
        const res = {
            status: () => ({
                end: endResponseStub
            })
        };

        const result = await fetchFromBigQuery(req, res);
        expect(result).to.be.undefined;
        expect(endResponseStub).to.have.been.calledOnce;
        expect(res.status().end.firstCall.args[0]).to.equal(`Invalid parameters => 'startDate', 'endDate' and 'eventTypes' are required`);
    });

    it(`should convert 'datetime string' to 'milliseconds integer' successfully`, async () => {
        const result1 = await convertDateTimeStringToMillisecondsInteger(startDateFilter, timeAtStartOfDay);
        expect(result1).to.exist;
        expect(result1).to.equal(dateConversionToMilliseconds[startDateFilter]);

        const result2 = await convertDateTimeStringToMillisecondsInteger(endDateFilter, timeAtEndOfDay);
        expect(result2).to.exist;
        expect(result2).to.equal(dateConversionToMilliseconds[endDateFilter]);
    });

    it(`should construct sql query to 'fetch amplitude data from big query' successfully`, async () => {
        const expectedQuery = `
            SELECT event_type, COUNT(event_type) as event_count
            FROM \`${GOOGLE_PROJECT_ID}.${DATASET}.${TABLE}\`
            where \`time_transaction_occurred\` BETWEEN @start_date and @end_date and \`source_of_event\`="${SOURCE_OF_EVENT}" 
            and event_type in UNNEST(@eventTypes) GROUP BY event_type
        `;
        const result = await constructSQLQueryForAmplitudeFetch();
        expect(result).to.exist;
        expect(result).to.equal(expectedQuery);
    });


    it(`should construct options to 'fetch amplitude data from big query' successfully`, async () => {
        const expectedQuery = `
            SELECT event_type, COUNT(event_type) as event_count
            FROM \`${GOOGLE_PROJECT_ID}.${DATASET}.${TABLE}\`
            where \`time_transaction_occurred\` BETWEEN @start_date and @end_date and \`source_of_event\`="${SOURCE_OF_EVENT}" 
            and event_type in UNNEST(@eventTypes) GROUP BY event_type
        `;

        const expectedOptions = {
            query: expectedQuery,
            location: BIG_QUERY_DATASET_LOCATION,
            params: {
                /* eslint-disable camelcase */
                start_date: convertDateTimeStringToMillisecondsInteger(
                    samplePayloadForFetchAmplitudeData.startDate, timeAtStartOfDay
                ),
                end_date: convertDateTimeStringToMillisecondsInteger(
                    samplePayloadForFetchAmplitudeData.endDate, timeAtEndOfDay
                ),
                /* eslint-enable camelcase */
                eventTypes: samplePayloadForFetchAmplitudeData.eventTypes
            }
        };

        const result = await constructOptionsForBigQueryFetch(samplePayloadForFetchAmplitudeData, expectedQuery);
        expect(result).to.exist;
        expect(result).to.deep.equal(expectedOptions);
    });

    it(`should 'fetch amplitude data from big query' successfully`, async () => {
        const req = {
            method: POST,
            body: { ...samplePayloadForFetchAmplitudeData}
        };
        const jsonResponseStub = sinon.stub();
        const res = {
            status: () => ({
                json: jsonResponseStub
            })
        };

        const result = await fetchFromBigQuery(req, res);
        expect(result).to.be.undefined;
        expect(jsonResponseStub).to.have.been.calledOnce;
        expect(res.status().json.firstCall.args[0]).to.deep.equal(extractedSampleResponseFromBigQuery);
    });

    it(`should handle failures gracefully while 'fetching amplitude data from big query' `, async () => {
            const req = {
                method: POST,
                body: null
            };
            const endResponseStub = sinon.stub();
            const res = {
                status: () => ({
                    end: endResponseStub
                })
            };

            const result = await fetchFromBigQuery(req, res);
            expect(result).to.be.undefined;
            expect(endResponseStub).to.have.been.calledOnce;
            expect(res.status().end.firstCall.args[0]).to.equal(`Unable to 'fetch amplitude data from big query'`);
    });

});
