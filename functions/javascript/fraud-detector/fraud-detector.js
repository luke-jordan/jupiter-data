// 1. TODO: takes user's transaction from Pub/Sub `sns-events`

// 2. TODO: Runs user-behaviour 
    // saves to table `user_behaviour`: table contains user transactions: deposits and withdrawals
    // Then retrieve properties to be used by rules engine. Properties include: last_transaction_amount

// 3. Run rules engine (https://www.npmjs.com/package/json-rules-engine) 
// TODO: define proper rules
const config = require('config');
const {BigQuery} = require('@google-cloud/bigquery');
const Engine = require('json-rules-engine').Engine;
const bigqueryClient = new BigQuery();

const DATASET_ID = config.get("BIG_QUERY_DATASET_ID");
const TABLE_ID = config.get("process.env.BIG_QUERY_TABLE_ID");

const EMAIL_TYPE = 'EMAIL';

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
        reasonForFlaggingUser: `User's last deposit was greater than 100,000 rands`
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
        reasonForFlaggingUser: `User has deposited 50,000 rands 3 or more times in the last 6 months`
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
  userAccountInfo: {
    userId: "1a",
    accountId: "3b43",
  },
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
    const reasonForFlaggingUser = event.params.reasonForFlaggingUser;
    const {
      userAccountInfo
    } = facts;
    console.log(reasonForFlaggingUser);
    // 4. If Flagged 
    // log to `user_flagged_as_fradulent`
    try {
        await logUserFlag(userAccountInfo, reasonForFlaggingUser);
    } catch (error) {
        console.log('error occured while logging user flag', error);
    }
        
    const notificationPayload = {
      notificationType: EMAIL_TYPE, 
      contacts: ['avish@plutosave.com', 'luke@plutosave.com'], 
      message: `User: ${userAccountInfo.userId} with account: ${userAccountInfo.accountId} has been flagged as fraudulent. Reason for flagging User: ${reasonForFlaggingUser}`
    };
    // tell Avish => email function accepts (email address and message)
    // TODO: send post request to notification-service

    console.log('sending notification request with payload: ', notificationPayload);
}

function createTimestampForSQLDatabase() {
  // courtesy: https://stackoverflow.com/questions/5129624/convert-js-date-time-to-mysql-datetime
  return new Date().toISOString().slice(0, 19).replace('T', ' ');
}

async function logUserFlag (userAccountInfo, reasonForFlaggingUser) {
    const {
      userId, 
      accountId 
    } = userAccountInfo
    const timestamp = createTimestampForSQLDatabase();
    const payloadForFlaggedTable = {
        user_id: userId,
        account_id: accountId,
        reason: reasonForFlaggingUser,
        accuracy: accuracyStates.pending,
        created_at: timestamp,
        updated_at: timestamp
    };

    const row = [
        payloadForFlaggedTable
      ];
      console.log(`Inserting user flag: ${JSON.stringify(row)} into database`);
  
      // Insert data into a table
      await bigqueryClient.
        dataset(DATASET_ID).
        table(TABLE_ID).
        insert(row);
        console.log(`Successfully inserted user flag: ${JSON.stringify(row)} into database`);
}
 
// Run the engine to evaluate
engine.
  run(facts).
  then((results) => results.events.map((event) => processSuccessResultOfRulesEngine(event)));


// TODO: Add verbose mode to config (if ON => send notification that says it ran)
// TODO: deploy service => add to circle ci and terraform