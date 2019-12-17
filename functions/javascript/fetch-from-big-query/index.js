// Import the Google Cloud client library
const {BigQuery} = require('@google-cloud/bigquery');
const bigqueryClient = new BigQuery();
const dotenv = require('dotenv');
dotenv.config();

// TODO: replace `dotenv` with `config`, this requires updating circle ci environment variables and the `config.yml`
// TODO: change name of this file from `index.js` to `fetch-from-amplitude` this requires changing the terraform file

const GOOGLE_PROJECT_ID = process.env.GOOGLE_PROJECT_ID;
const BIG_QUERY_DATASET_LOCATION = process.env.BIG_QUERY_DATASET_LOCATION;
const DATASET = 'ops';
const TABLE = 'all_user_events';
const timeAtStartOfDay = [0, 0, 0];
const timeAtEndOfDay = [23, 59, 59];
const SOURCE_OF_EVENT = 'AMPLITUDE';

function confirmRequiredParameters(parameters) {
    return !parameters.startDate || !parameters.endDate
        || !parameters.eventTypes || !Array.isArray(parameters.eventTypes);
}

const convertDateTimeStringToMillisecondsInteger = (dateString, time) => {
    console.log(`Converting date time string: ${dateString} to milliseconds integer`);
    const dateTimeArray = dateString.split('-');
    const year =  parseInt(dateTimeArray[0]);
    const month =  parseInt(dateTimeArray[1]) - 1; // months are expected in form: `0` to `11` i.e. `Jan` to `Dec`
    const day =  parseInt(dateTimeArray[2]);
    const hour = parseInt(time[0]);
    const minute = parseInt(time[1]);
    const second = parseInt(time[2]);

    const millisecondInteger = Date.UTC(year, month, day, hour, minute, second);
    console.log(
        `Successfully converted date time string: ${dateString} to milliseconds integer: ${millisecondInteger}`
    );
    return millisecondInteger;
};

exports.fetchFromBigQuery = async function fetchFromBigQuery(req, res) {
    if (req.method !== 'POST') {
        res.status(405).end('only post method accepted');
        return;
    }

    try {
        const parameters = JSON.parse(JSON.stringify(req.body));

        console.log('parameters received', parameters);

        if (confirmRequiredParameters(parameters)) {
            res.status(400).end(`Invalid parameters => 'startDate', 'endDate' and 'eventTypes' are required`);
            return;
        }

        const {
            startDate,
            endDate,
            eventTypes
        } = parameters;

        console.log('constructing sql query');
        // The SQL query to run
        const sqlQuery = `SELECT event_type, COUNT(event_type) as event_count
                    FROM \`${GOOGLE_PROJECT_ID}.${DATASET}.${TABLE}\`
                    where \`timestamp\` BETWEEN @start_date and @end_date and source_of_event = ${SOURCE_OF_EVENT} 
                    and event_type in (@eventTypes) GROUP BY event_type`;

        console.log(`sql query to be run: ${sqlQuery}`);

        const options = {
            query: sqlQuery,
            // Location must match that of the dataset(s) referenced in the query.
            location: BIG_QUERY_DATASET_LOCATION,
            params: {
                start_date: convertDateTimeStringToMillisecondsInteger(startDate, timeAtStartOfDay),
                end_date: convertDateTimeStringToMillisecondsInteger(endDate, timeAtEndOfDay),
                eventTypes
            },
        };

        // Run the query
        const [rows] = await bigqueryClient.query(options);

        console.log(`Response from big query: ${JSON.stringify(rows)}`);
        res.status(200).json(rows);
        return;
    } catch (error) {
        const errorMessage = 'Error while fetching from big query';
        console.log(errorMessage, error);
        res.status(400).end(errorMessage);
    }
};