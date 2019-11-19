// Import the Google Cloud client library
const {BigQuery} = require('@google-cloud/bigquery');
const bigqueryClient = new BigQuery();
const dotenv = require('dotenv');
dotenv.config();
const GOOGLE_PROJECT_ID = process.env.GOOGLE_PROJECT_ID;
const BIG_QUERY_DATASET_LOCATION = process.env.BIG_QUERY_DATASET_LOCATION;
const DATASET = 'amplitude';
const TABLE = 'events';

function confirmRequiredParameters(parameters) {
    return !parameters.startDate || !parameters.endDate
        || !parameters.eventTypes || !Array.isArray(parameters.eventTypes);
}

function formatEventTypesForSQLQuery(eventTypes) {
    return JSON.stringify(eventTypes)
        .replace('[', '(')
        .replace(']', ')')
}

exports.fetchFromBigQuery = async function fetchFromBigQuery(req, res) {
    if (req.method !== 'POST') {
        res.status(405).end('only post method accepted');
        return;
    }

    try {
        const parameters = JSON.parse(JSON.stringify(req.body));

        console.log('parameters received', parameters);

        if (confirmRequiredParameters(parameters)) {
            res.status(400).end('invalid parameters => startDate, endDate and eventTypes are required');
            return;
        }

        const {
            startDate,
            endDate,
            eventTypes
        } = parameters;

        const eventTypesInSQLFormat = formatEventTypesForSQLQuery(eventTypes);

        console.log('constructing sql query');
        // The SQL query to run
        const sqlQuery = `SELECT event_type, COUNT(event_type) as event_count
                    FROM \`${GOOGLE_PROJECT_ID}.${DATASET}.${TABLE}\`
                    where DATE(client_event_time) BETWEEN @start_date and @end_date and event_type in `
                    +  eventTypesInSQLFormat + ` GROUP BY event_type`;

        console.log(`sql query to be run: ${sqlQuery}`);

        const options = {
            query: sqlQuery,
            // Location must match that of the dataset(s) referenced in the query.
            location: BIG_QUERY_DATASET_LOCATION,
            params: { start_date: startDate, end_date: endDate },
        };

        // Run the query
        const [rows] = await bigqueryClient.query(options);

        console.log(`Response from big query: ${JSON.stringify(rows)}`);
        res.status(200).json(rows);
        return;
    } catch (error) {
        const errorMessage = 'error while fetching from big query';
        console.log(errorMessage, error);
        res.status(400).end('error while fetching from big query');
    }
};