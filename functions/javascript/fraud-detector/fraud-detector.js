'use strict';

const config = require('config');
const {BigQuery} = require('@google-cloud/bigquery');
const Engine = require('json-rules-engine').Engine;
const logger = require('debug')('jupiter:notification-service');
const requestRetry = require('requestretry');
const httpStatus = require('http-status');
const DATASET_ID = config.get('BIG_QUERY_DATASET_ID');
const TABLE_ID = config.get('BIG_QUERY_TABLE_ID');
const CUSTOM_RULES = require('./custom-rules').allRules;
const serviceUrls = config.get('serviceUrls');
const EMAIL_SUBJECT_FOR_ADMINS = config.get('emailSubjectForAdmins');
const {
    createTimestampForSQLDatabase
} = require('./utils');

const {
    NOTIFICATION_SERVICE_URL,
    FETCH_USER_BEHAVIOUR_URL
} = serviceUrls;

const {
    accuracyStates,
    httpMethods,
    notificationTypes,
    baseConfigForRequestRetry,
    requestTitle
} = require('./constants');

const { EMAIL_TYPE } = notificationTypes;
const {
    POST,
    GET
} = httpMethods;
const {
    NOTIFICATION,
    FETCH_USER_BEHAVIOUR
} = requestTitle;
const CONTACTS_TO_BE_NOTIFIED = config.get('contactsToBeNotified');
const VERBOSE_MODE = config.get('verboseMode');
const bigQueryClient = new BigQuery();

const sendHttpRequest = async (extraConfig, specifiedRequestTitle) => {
    logger(`sending '${specifiedRequestTitle}' request with extra config: ${JSON.stringify(extraConfig)}`);

    const response = await requestRetry({
        ...baseConfigForRequestRetry,
        ...extraConfig
    });

    logger(`Response from '${specifiedRequestTitle}' request with extra config: ${JSON.stringify(extraConfig)}. 
    Response: ${JSON.stringify(response)}`);
    return response;
};

const constructPayloadForUserFlagTable = (userAccountInfo, reasonForFlaggingUser) => {
    logger(`constructing payload for user flag table with user info: ${JSON.stringify(userAccountInfo)}`);
    const {
        userId,
        accountId
    } = userAccountInfo;
    const timestamp = createTimestampForSQLDatabase();

    /*eslint-disable */
    /**
     * eslint disable is needed to turn off the eslint rule: 'camelcase'
     * The below values represent the columns of the user_flag tables and the column names are not in camelcase
     */
    const payloadForFlaggedTable = {
        user_id: userId,
        account_id: accountId,
        reason: reasonForFlaggingUser,
        accuracy: accuracyStates.pending,
        created_at: timestamp,
        updated_at: timestamp
    };
    /* eslint-enable */

    return [
        payloadForFlaggedTable
    ];
};

const isRuleSafeOrIsExperimentalModeOn = (rule) => {
    if (config.has('experimentalRules.run') && Boolean(config.get('experimentalRules.run'))) {
        return true;
    }

    return !Reflect.has(rule, 'experimental') && !Boolean(rule.experimental);
};

const constructNewEngineAndAddRules = (rules) => {
    logger(`constructing new engine and adding new rules to the engine. Rules: ${JSON.stringify(rules)}`);

    /**
     * Setup a new engine
     */
    const engine = new Engine();

    rules.filter((rule) => isRuleSafeOrIsExperimentalModeOn(rule)).
        forEach((rule) => engine.addRule(rule));
    return engine;
};

const notifyAdminsOfNewlyFlaggedUser = async (payload) => {
    logger('notifying admins of newly flagged user');
    try {
        const extraConfig = {
            url: `${NOTIFICATION_SERVICE_URL}`,
            method: POST,
            body: payload
        };
        await sendHttpRequest(extraConfig, NOTIFICATION);
    } catch (error) {
        logger(`Error occurred while notifying admins of newly flagged user. Error: ${JSON.stringify(error)}`);
    }
};

const insertUserFlagIntoTable = async (row) => {
    try {
        logger(`Inserting user flag: ${JSON.stringify(row)} into database`);

        await bigQueryClient.
        dataset(DATASET_ID).
        table(TABLE_ID).
        insert(row);

        logger(`Successfully inserted user flag: ${JSON.stringify(row)} into database`);
    } catch (error) {
        logger(`Error occurred while saving user flag to big query. Error: ${JSON.stringify(error)}`);
    }
};

const logFraudulentUserFlag = async (userAccountInfo, reasonForFlaggingUser) => {
    logger(`save new fraudulent flag for user with info: ${JSON.stringify(userAccountInfo)} for reason: '${reasonForFlaggingUser}'`);
    const row = constructPayloadForUserFlagTable(userAccountInfo, reasonForFlaggingUser);

    await insertUserFlagIntoTable(row);
};

const constructNotificationPayload = (userAccountInfo, reasonForFlaggingUser) => ({
        notificationType: EMAIL_TYPE,
        contacts: CONTACTS_TO_BE_NOTIFIED,
        message: `User: ${userAccountInfo.userId} with account: ${userAccountInfo.accountId} has been flagged as fraudulent. Reason for flagging User: ${reasonForFlaggingUser}`,
        subject: EMAIL_SUBJECT_FOR_ADMINS
    });

const extractReasonForFlaggingUserFromEvent = (event) => event.params.reasonForFlaggingUser;

const logFraudulentUserAndNotifyAdmins = async (event, userAccountInfo) => {
    logger(`Processing success result of rules engines with event: ${JSON.stringify(event)}`);
    logger(`log fraudulent user and notify admins. User Info: ${JSON.stringify(userAccountInfo)}`);
    // 'results' is an object containing successful events, and an Almanac instance containing facts
    const reasonForFlaggingUser = extractReasonForFlaggingUserFromEvent(event);
    
    // log to `user_flagged_as_fraudulent`
    await logFraudulentUserFlag(userAccountInfo, reasonForFlaggingUser);
        
    const notificationPayload = constructNotificationPayload(userAccountInfo, reasonForFlaggingUser);

    // tell Avish => email function accepts (email address and message)
    await notifyAdminsOfNewlyFlaggedUser(notificationPayload, NOTIFICATION);
    
};

// Run the engine to evaluate facts against the rules
const createEngineAndRunFactsAgainstRules = (facts, rules) => {
    const engineWithRules = constructNewEngineAndAddRules(CUSTOM_RULES);

    logger(`Running facts: ${JSON.stringify(facts)} against rules: ${JSON.stringify(rules)}`);

    engineWithRules.
    run(facts).
    then((resultsOfPassedRules) => resultsOfPassedRules.events.forEach(async (event) => {
        await logFraudulentUserAndNotifyAdmins(event, facts.userAccountInfo);
    })).catch((error) => {
       logger(`Error occurred while Running engine with facts: ${JSON.stringify(facts)} and rules: ${JSON.stringify(rules)}. Error: ${JSON.stringify(error)}`);
        });
};

const fetchFactsFromUserBehaviourService = async (userId, accountId) => {
    logger(
        `Fetching facts from user behaviour service for user id: ${userId} and account id: ${accountId}`
    );
    try {
        const extraConfig = {
            url: `${FETCH_USER_BEHAVIOUR_URL}?userId=${userId}&accountId=${accountId}`,
            method: GET
        };
        const response = await sendHttpRequest(extraConfig, FETCH_USER_BEHAVIOUR);
        logger(`Successfully fetched facts from user behaviour. Facts: ${JSON.stringify(response.body)}`);

        if (!response || !response.body) {
            throw 'Could not fetch facts from user behaviour';
        }

        return response.body;
    } catch (error) {
        logger(`Error occurred while fetching facts from user behaviour service for user id: ${userId}. Error: ${JSON.stringify(error)}`);
        throw error;
    }
};

const sendFailureResponse = (res, error) => {
    logger(`Error occurred while handling 'check for fraudulent user' request. Error: ${JSON.stringify(error)}`);
    res.status(httpStatus.BAD_REQUEST).end('Unable to check for fraudulent user');
};

const sendSuccessResponse = (req, res) => {
    logger(`Received request to 'check for fraudulent user' request. Payload: ${JSON.stringify(req.body)}`);
    res.status(httpStatus.OK).end(`Received request to 'check for fraudulent user'`);
};

const handleNotSupportedHttpMethod = (res) => {
    logger(`Request is invalid. Only ${POST} http method accepted`);
    res.status(httpStatus.METHOD_NOT_ALLOWED).end(`Only ${POST} http method accepted`);
};

const missingParameterInReceivedPayload = (parameters) => !parameters.userId || !parameters.accountId;

const handleMissingParameterInReceivedPayload = (payload, res) => {
    res.status(httpStatus.BAD_REQUEST).end(`invalid payload => 'userId' and 'accountId' are required`);
    logger(
        `Request to 'check for fraudulent user' failed because of invalid parameters in received payload. 
        Received payload: ${JSON.stringify(payload)}`
    );
    return null;
};

const validateRequestAndExtractParams = (req, res) => {
    if (req.method !== POST) {
        return handleNotSupportedHttpMethod(res);
    }

    try {
        const payload = JSON.parse(JSON.stringify(req.body));
        if (missingParameterInReceivedPayload(payload)) {
            return handleMissingParameterInReceivedPayload(payload, res);
        }
        return payload;
    } catch (error) {
        sendFailureResponse(res, error);
    }
};

const sendNotificationOfResultToAdmins = async (requestStatus) => {
    logger('Notifying admins of result (error, or verbose mode, or...)');
    const baseMessage = 'Just so you know, fraud detector ran';
    const message = requestStatus.info ?
        `${baseMessage}. Extra info: ${JSON.stringify(requestStatus.info)}`: baseMessage;
    const payload = {
        subject: `[${requestStatus.result}] => Fraud Detector just ran`,
        message,
        contacts: CONTACTS_TO_BE_NOTIFIED,
        notificationType: EMAIL_TYPE
    };
    try {
        const extraConfig = {
            url: `${NOTIFICATION_SERVICE_URL}`,
            method: POST,
            body: payload
        };
        await sendHttpRequest(extraConfig, NOTIFICATION);
    } catch (error) {
        logger(`Error occurred while notifying admins that fraud detector ran (Verbose Mode). Error: ${JSON.stringify(error)}`);
    }
};


const fetchFactsAboutUserAndRunEngine = async (req, res) => {
    sendSuccessResponse(req, res);
    try {
        const payload = validateRequestAndExtractParams(req, res);
        if (!payload) {
            return;
        }

        const factsAboutUser = await fetchFactsFromUserBehaviourService(payload.userId, payload.accountId);

        await createEngineAndRunFactsAgainstRules(factsAboutUser, CUSTOM_RULES);

        if (VERBOSE_MODE) {
            await sendNotificationOfResultToAdmins({ result: 'SUCCESS'});
        }
    } catch (error) {
        logger(
            `Error occurred while fetching facts about user and running engine with facts/rules.
            Error: ${JSON.stringify(error)}`
        );

        await sendNotificationOfResultToAdmins({ result: 'FAILED', info: error });
    }
};

module.exports = {
    logFraudulentUserAndNotifyAdmins,
    extractReasonForFlaggingUserFromEvent,
    logFraudulentUserFlag,
    createEngineAndRunFactsAgainstRules,
    constructNotificationPayload,
    notifyAdminsOfNewlyFlaggedUser,
    constructPayloadForUserFlagTable,
    insertUserFlagIntoTable,
    fetchFactsAboutUserAndRunEngine,
    sendNotificationForVerboseMode: sendNotificationOfResultToAdmins
};
