// 2. TODO: fetch properties from user-behaviour 
    // Then retrieve properties to be used by rules engine. Properties include: last_transaction_amount
'use strict';

// 3. Run rules engine (https://www.npmjs.com/package/json-rules-engine) 
// TODO: define proper rules
const config = require('config');
const {BigQuery} = require('@google-cloud/bigquery');
const Engine = require('json-rules-engine').Engine;
const logger = require('debug')('jupiter:notification-service');
const requestRetry = require('requestretry');

const DATASET_ID = config.get("BIG_QUERY_DATASET_ID");
const TABLE_ID = config.get("BIG_QUERY_TABLE_ID");

const {
    createTimestampForSQLDatabase
} = require('./utils');
const {
    accuracyStates,
    httpMethods,
    notificationTypes,
    delayForHttpRequestRetries,
    requestTitle
} = require('./constants');

const { EMAIL_TYPE } = notificationTypes;
const { POST } = httpMethods;
const { NOTIFICATION } = requestTitle;
const CONTACTS_TO_BE_NOTIFIED = config.get('contactsToBeNotified');
const bigQueryClient = new BigQuery();


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

const constructNewEngineAndAddRules = (rules) => {
    /**
     * Setup a new engine
     */
    const engine = new Engine();
    rules.forEach(rule => engine.addRule(rule));
    return engine;
};

const notifyAdminsOfNewlyFlaggedUser = async (payload) => {
    logger('notifying admins of newly flagged user');
    try {
        logger(`sending '${NOTIFICATION}' request with payload: ${JSON.stringify(payload)}`);
        const response = await requestRetry({
            url: `{base_url}/helloHttp`,
            method: POST,
            body: payload,
            retryDelay: delayForHttpRequestRetries,
            json: true,
        });

        logger(`response from ${NOTIFICATION} request with payload: ${payload}. Response: ${JSON.stringify(response)}`);
    } catch (error) {
        logger('error occurred while logging user flag', error);
    }
};

const constructNotificationPayload = (userAccountInfo, reasonForFlaggingUser) => {
    return {
        notificationType: EMAIL_TYPE,
        contacts: CONTACTS_TO_BE_NOTIFIED,
        message: `User: ${userAccountInfo.userId} with account: ${userAccountInfo.accountId} has been flagged as fraudulent. Reason for flagging User: ${reasonForFlaggingUser}`
    }
};

const extractReasonForFlaggingUserFromEvent =  (event) => event.params.reasonForFlaggingUser;

const logFraudulentUserAndNotifyAdmins = async (event, userAccountInfo) => {
    logger(`Processing success result of rules engines with event: ${JSON.stringify(event)}`);
    // 'results' is an object containing successful events, and an Almanac instance containing facts
    const reasonForFlaggingUser = extractReasonForFlaggingUserFromEvent(event);
    
    // log to `user_flagged_as_fraudulent`
    await logFraudulentUserFlag(userAccountInfo, reasonForFlaggingUser);
        
    const notificationPayload = constructNotificationPayload(userAccountInfo, reasonForFlaggingUser);

    // tell Avish => email function accepts (email address and message)
    await notifyAdminsOfNewlyFlaggedUser(notificationPayload, NOTIFICATION);
    return;
};


const logFraudulentUserFlag = async (userAccountInfo, reasonForFlaggingUser) => {
    logger(`save new fraudulent flag for user with info: ${userAccountInfo} for reason: ${reasonForFlaggingUser}`)
    const {
      userId, 
      accountId 
    } = userAccountInfo;
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
      logger(`Inserting user flag: ${JSON.stringify(row)} into database`);

    try {
        // Insert data into a table
        await bigQueryClient.
        dataset(DATASET_ID).
        table(TABLE_ID).
        insert(row);

        logger(`Successfully inserted user flag: ${JSON.stringify(row)} into database`);
    } catch (error) {
        logger('error occurred while logging user flag', error);
    }
};

/**
 * Define facts the engine will use to evaluate the conditions above.
 * Facts may also be loaded asynchronously at runtime; see the advanced example below
 */
const exampleFacts = {
    userAccountInfo: {
        userId: "1a",
        accountId: "3b43",
    },
    lastDeposit: 10000,
    depositsLargerThanBaseIn6months: 4
};
const exampleRules = [customRule1, customRule2];

// Run the engine to evaluate facts against the rules
const runEngine = (facts, rules) => {
    const engine = constructNewEngineAndAddRules(rules);

    console.log('engine', engine);
    engine.
    run(facts).
    then((resultsOfPassedRules) => resultsOfPassedRules.events.forEach((event) => logFraudulentUserAndNotifyAdmins(event, facts.userAccountInfo)));
};

module.exports = {
    logFraudulentUserAndNotifyAdmins,
    runEngine
};


// TODO: Add verbose mode to config (if ON => send notification that says it ran)
// TODO: deploy service => add to circle ci and terraform