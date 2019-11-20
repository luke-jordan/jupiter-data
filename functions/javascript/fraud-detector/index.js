// 1. TODO: takes user's transaction from Pub/Sub `sns-events`

// 2. TODO: Runs user-behaviour 
    // saves to table `user_behaviour`: table contains user transactions: deposits and withdrawals
    // Then retrieve properties to be used by rules engine. Properties include: last_transaction_amount


// 3. Runs rules engine (https://www.npmjs.com/package/json-rules-engine) 

const Engine = require('json-rules-engine').Engine;
const {BigQuery} = require('@google-cloud/bigquery');
const bigqueryClient = new BigQuery();
const dotenv = require('dotenv');
dotenv.config();
const DATASET_ID = process.env.BIG_QUERY_DATASET_ID;
const TABLE_ID = process.env.BIG_QUERY_TABLE_ID;

/**
 * Setup a new engine
 */
const engine = new Engine();
const customRule1 = {
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
        message: 'User has been flagged as fraudulent, please check the user out',
        reason: `user's last deposit was greater than 100000 rands`
      }
    }
};

const customRule2 = {
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
        message: 'User has been flagged as fraudulent, please check the user out',
        reason: `user has deposited 50000 rands 3 or more times in the last 6 months`
      }
    }
};
 
// define a rule for detecting the player has exceeded foul limits.  Foul out any player who:
// (has committed 5 fouls AND game is 40 minutes) OR (has committed 6 fouls AND game is 48 minutes)
engine.addRule(customRule1);
engine.addRule(customRule2);

 
/**
 * Define facts the engine will use to evaluate the conditions above.
 * Facts may also be loaded asynchronously at runtime; see the advanced example below
 */
const facts = {
  userId: 3,
  lastDeposit: 10000,
  depositsLargerThanBaseIn6months: 4
};

const accuracyStates = {
    pending: 'PENDING_CONFIRMATION',
    falseAlarm: 'FALSE_ALARM',
    accuratePrediction: 'ACCURATE_PREDICTION'
};

async function processSuccessResultOfRulesEngine (event) {
    // 'results' is an object containing successful events, and an Almanac instance containing facts
    const reason = event.params.reason;
    const userId = facts.userId;
    console.log(reason);
    // 4. If Flagged 
    // log to `user_flagged_as_fradulent`
    try {
        await logUserFlag(userId, reason);
    } catch (error) {
        console.log('error occured while logging user flag', error);
    }
        
    // tell Avish => email function accepts (email address and message)
    // TODO: Write notification function (primarily email)

    console.log(event.params.message);
}

async function logUserFlag (userId, reason) {
    const timestampNow = new Date().toISOString().slice(0, 19).replace('T', ' '); // courtesy: https://stackoverflow.com/questions/5129624/convert-js-date-time-to-mysql-datetime
    const payloadForFlaggedTable = {
        user_id: userId,
        reason,
        accuracy: accuracyStates.pending,
        created_at: timestampNow,
        updated_at: timestampNow
    };

    const rows = [
        payloadForFlaggedTable
      ];
      console.log(`Inserting user flag: ${rows} into database`);
  
      // Insert data into a table
      await bigqueryClient.
        dataset(DATASET_ID).
        table(TABLE_ID).
        insert(rows);
        console.log(`Successfully inserted user flag: ${rows} into database`);
}
 
// Run the engine to evaluate
engine.
  run(facts).
  then((results) => results.events.map((event) => processSuccessResultOfRulesEngine(event)));
