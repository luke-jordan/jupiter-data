'use strict';

const {BigQuery} = require('@google-cloud/bigquery');
const bigqueryClient = new BigQuery();
const logger = require('debug')('jupiter:');
const httpStatus = require('http-status');

const dotenv = require('dotenv');
dotenv.config();

// TODO: replace `dotenv` with `config`, this requires updating circle ci environment variables and the `config.yml`
// TODO: change name of this file from `index.js` to `fetch-from-amplitude` this requires changing the terraform file

const constants = require('./constants');
const {
    httpMethods,
    DATASET,
    TABLE,
    timeAtStartOfDay,
    timeAtEndOfDay,
    SOURCE_OF_EVENT,
    DEFAULT_RADIX_PARAMETER
} = constants;
const {
    POST
} = httpMethods;

const GOOGLE_PROJECT_ID = process.env.GOOGLE_PROJECT_ID;
const BIG_QUERY_DATASET_LOCATION = process.env.BIG_QUERY_DATASET_LOCATION;

const convertDateTimeStringToMillisecondsInteger = (dateString, timeString) => {
    logger(`Converting date time string: ${dateString} to milliseconds integer`);

    // assigned this variables to satisfy eslint
    const ONE = 1;
    const THIRD_ELEMENT = 2;

    const dateArray = dateString.split('-');
    const year = parseInt(dateArray[0], DEFAULT_RADIX_PARAMETER);
    const month = parseInt(dateArray[1], DEFAULT_RADIX_PARAMETER) - ONE; // months are expected in form: `0` to `11` i.e. `Jan` to `Dec`
    const day = parseInt(dateArray[THIRD_ELEMENT], DEFAULT_RADIX_PARAMETER);
    
    const timeArray = timeString.split(':');
    const hour = parseInt(timeArray[0], DEFAULT_RADIX_PARAMETER);
    const minute = parseInt(timeArray[1], DEFAULT_RADIX_PARAMETER);
    const second = parseInt(timeArray[THIRD_ELEMENT], DEFAULT_RADIX_PARAMETER);

    const millisecondInteger = Date.UTC(year, month, day, hour, minute, second);
    logger(
        `Successfully converted date time string: ${dateString} to milliseconds integer: ${millisecondInteger}`
    );
    return millisecondInteger;
};

const missingParameterInReceivedPayload = (parameters) => !parameters.startDate || !parameters.endDate ||
        !parameters.eventTypes || !Array.isArray(parameters.eventTypes);

const handleMissingParameterInReceivedPayload = (payload, res) => {
    res.status(httpStatus.BAD_REQUEST).end(`Invalid parameters => 'startDate', 'endDate' and 'eventTypes' are required`);
    logger(
        `Request to 'fetch amplitude data from big query' failed because of invalid parameters in received payload. 
        Received payload: ${JSON.stringify(payload)}`
    );
    return null;
};

const handleNotSupportedHttpMethod = (res) => {
    logger(`Request is invalid. Only ${POST} http method accepted`);
    res.status(httpStatus.METHOD_NOT_ALLOWED).end(`Only ${POST} http method accepted`);
};

const sendFailureResponse = (res, error) => {
    logger(`Error occurred while handling 'fetching amplitude data from big query' request. 
    Error: ${JSON.stringify(error.message)}`);
    res.status(httpStatus.BAD_REQUEST).end(`Unable to 'fetch amplitude data from big query'`);
};

const validateRequestAndExtractParams = (req, res) => {
    if (req.method !== POST) {
        return handleNotSupportedHttpMethod(res);
    }

    try {
        const payload = JSON.parse(JSON.stringify(req.body));
        logger(`Parameters received: ${JSON.stringify(payload)}`);

        if (missingParameterInReceivedPayload(payload)) {
            return handleMissingParameterInReceivedPayload(payload, res);
        }
        return payload;
    } catch (error) {
        logger(`Error while json parsing body of request. Request Body: ${JSON.stringify(req.body)}`);
        throw error;
    }
};

const constructSQLQueryForAmplitudeFetch = () => {
    const sqlQuery = `
            SELECT event_type, COUNT(event_type) as event_count
            FROM \`${GOOGLE_PROJECT_ID}.${DATASET}.${TABLE}\`
            where \`time_transaction_occurred\` BETWEEN @start_date and @end_date and \`source_of_event\`="${SOURCE_OF_EVENT}" 
            and event_type in UNNEST(@eventTypes) GROUP BY event_type
        `;
    logger(`SQL query to be run: ${sqlQuery}`);
    return sqlQuery;
};

const constructOptionsForBigQueryFetch = async (payload) => {
    logger(`Constructing options for amplitude fetch with payload: ${JSON.stringify(payload)}`);

    const sqlQuery = await constructSQLQueryForAmplitudeFetch();

    const {
        startDate,
        endDate,
        eventTypes
    } = payload;


    return {
        query: sqlQuery,
        location: BIG_QUERY_DATASET_LOCATION,
        params: {
            /* eslint-disable camelcase */
            start_date: convertDateTimeStringToMillisecondsInteger(startDate, timeAtStartOfDay),
            end_date: convertDateTimeStringToMillisecondsInteger(endDate, timeAtEndOfDay),
            /* eslint-enable camelcase */
            eventTypes
        }
    };
};

const fetchFromBigQuery = async (req, res) => {
    try {
        const payload = validateRequestAndExtractParams(req, res);
        if (!payload) {
            return;
        }

        const options = await constructOptionsForBigQueryFetch(payload);

        logger(`Fetching amplitude data from big query table: ${TABLE}`);
        const [rows] = await bigqueryClient.query(options);
        logger(`Successfully fetched amplitude data from big query. Response: ${JSON.stringify(rows)}`);
        res.status(httpStatus.OK).json(rows);
        return;
    } catch (error) {
        sendFailureResponse(res, error);
    }
};

module.exports = {
    fetchFromBigQuery,
    constructSQLQueryForAmplitudeFetch,
    convertDateTimeStringToMillisecondsInteger,
    constructOptionsForBigQueryFetch
};
