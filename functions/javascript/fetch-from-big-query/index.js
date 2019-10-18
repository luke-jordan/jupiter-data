// Import the Google Cloud client library
const {BigQuery} = require('@google-cloud/bigquery');
const bigqueryClient = new BigQuery();

function confirmRequiredParameters(parameters) {
    return !parameters.start_date || !parameters.end_date
        || !parameters.event_types || !Array.isArray(parameters.event_types);
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
            console.log('missing value');
            // res.status(400).end('invalid parameters => start_date, end_date and event_types are required');
            return;
        }

        const {
            start_date,
            end_date,
            event_types
        } = parameters;

        const eventTypesInSQLFormat = formatEventTypesForSQLQuery(event_types);

        console.log('constructing sql query');
        // The SQL query to run
        const sqlQuery = `SELECT event_type, COUNT(event_type) as event_count
                    FROM \`jupiter-ml-alpha.amplitude.events\`
                    where DATE(_PARTITIONTIME) BETWEEN @startDate and @endDate and event_type in `
                    +  eventTypesInSQLFormat + ` GROUP BY event_type`;

        console.log(`sql query to be run: ${sqlQuery}`);

        const options = {
            query: sqlQuery,
            // Location must match that of the dataset(s) referenced in the query.
            location: 'US',
            params: {startDate: start_date, endDate: end_date, eventTypes: event_types },
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