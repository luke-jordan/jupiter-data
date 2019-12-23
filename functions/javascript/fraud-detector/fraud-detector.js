'use strict';

const config = require('config');
const {BigQuery} = require('@google-cloud/bigquery');
const Engine = require('json-rules-engine').Engine;
const logger = require('debug')('jupiter:notification-service');
const requestRetry = require('requestretry');
const httpStatus = require('http-status');
const GOOGLE_PROJECT_ID = config.get('GOOGLE_PROJECT_ID');
const BIG_QUERY_DATASET_LOCATION = config.get('BIG_QUERY_DATASET_LOCATION');
const DATASET_ID = config.get('BIG_QUERY_DATASET_ID');
const TABLE_ID = config.get('BIG_QUERY_TABLE_ID');
const CUSTOM_RULES = require('./custom-rules').allRules;
const serviceUrls = config.get('serviceUrls');
const EMAIL_SUBJECT_FOR_ADMINS = config.get('emailSubjectForAdmins');

const {
    generateCurrentTimeInMilliseconds
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
    POST
} = httpMethods;
const {
    NOTIFICATION,
    FETCH_USER_BEHAVIOUR
} = requestTitle;
const CONTACTS_TO_BE_NOTIFIED = config.get('contactsToBeNotified');
const VERBOSE_MODE = config.get('verboseMode');
const bigQueryClient = new BigQuery();

const sendHttpRequest = async (extraConfig, specifiedRequestTitle) => {
    logger(`Sending '${specifiedRequestTitle}' request with extra config: ${JSON.stringify(extraConfig)}`);

    const response = await requestRetry({
        ...baseConfigForRequestRetry,
        ...extraConfig
    });

    logger(`Response from '${specifiedRequestTitle}' request with extra config: ${JSON.stringify(extraConfig)}. 
    Response: ${JSON.stringify(response)}`);
    return response;
};

const constructPayloadForUserFlagTable = (userAccountInfo, ruleLabel, reasonForFlaggingUser) => {
    logger(`Constructing payload for user flag table with user info: ${JSON.stringify(userAccountInfo)}`);
    const {
        userId,
        accountId
    } = userAccountInfo;
    const currentTimeInMilliseconds = generateCurrentTimeInMilliseconds();

    /*eslint-disable */
    /**
     * eslint disable is needed to turn off the eslint rule: 'camelcase'
     * The below values represent the columns of the user_flag tables and the column names are not in camelcase
     */
    const payloadForFlaggedTable = {
        user_id: userId,
        account_id: accountId,
        rule_label: ruleLabel,
        reason: reasonForFlaggingUser,
        accuracy: accuracyStates.pending,
        created_at: currentTimeInMilliseconds,
        updated_at: currentTimeInMilliseconds
    };
    /* eslint-enable */

    return [
        payloadForFlaggedTable
    ];
};

const allowAllRulesSinceGeneralExperimentalModeIsOn = () => config.has('experimentalRules.run') && Boolean(config.get('experimentalRules.run'));

const allowOnlyNonExperimentalRules = (rule) => !Reflect.has(rule, 'experimental') || !rule.experimental;

const isRuleSafeOrIsExperimentalModeOn = (rule) => {
    if (allowAllRulesSinceGeneralExperimentalModeIsOn()) {
        return true;
    }

    return allowOnlyNonExperimentalRules(rule);
};

const constructNewEngineAndAddRules = (rules) => {
    logger(`Constructing new engine and adding new rules to the engine. Rules: ${JSON.stringify(rules)}`);

    const engine = new Engine();

    rules.filter((rule) => isRuleSafeOrIsExperimentalModeOn(rule)).
        forEach((rule) => engine.addRule(rule));
    return engine;
};

const attachRuleLabelsToLatestFlagTime = (ruleListWithLatestFlagTime) => {
    logger('Attaching rule labels to latest flag time');
    const formattedAlertTimes = {};
    ruleListWithLatestFlagTime.forEach((row) => {
        formattedAlertTimes[row.rule_label] = row.latest_flag_time.value;
    });
    return formattedAlertTimes;
};

const obtainLastAlertTimesForUser = async (rules, userId) => {
    const ruleLabels = rules.map((rule) => rule.event.params.ruleLabel);
    logger(`Obtaining last alert times for user with id: ${userId} and rule labels: ${JSON.stringify(ruleLabels)}`);

    const sqlQuery = `
        select \`user_id\`, \`rule_label\`, max(\`created_at\`) as \`latest_flag_time\` 
        from \`${GOOGLE_PROJECT_ID}.${DATASET_ID}.${TABLE_ID}\` 
        where \`user_id\` = @userId
        and \`rule_label\` in UNNEST(@ruleLabels)
        group by \`user_id\`, \`rule_label\`;
        `;

    const options = {
        query: sqlQuery,
        location: BIG_QUERY_DATASET_LOCATION,
        params: {
            userId,
            ruleLabels
        }
    };

    const [ruleListWithLatestFlagTime] = await bigQueryClient.query(options);

    let formattedAlertTimes = {};
    if (ruleListWithLatestFlagTime && ruleListWithLatestFlagTime.length > 0) {
        logger(
            `Successfully obtained last alert times for user with id: ${userId} and rule labels: ${JSON.stringify(ruleLabels)}.
        Alert times: ${JSON.stringify(ruleListWithLatestFlagTime)}`
        );

        formattedAlertTimes = attachRuleLabelsToLatestFlagTime(ruleListWithLatestFlagTime);
    }

    logger(
        `Formatted last alert times with latest flag dates for user with id: ${userId}.
        Formatted alert times: ${JSON.stringify(formattedAlertTimes)}`
    );

    return formattedAlertTimes;
};

const notifyAdminsOfNewlyFlaggedUser = async (payload) => {
    logger('Notifying admins of newly flagged user');
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

        logger(`Successfully inserted user flag: ${JSON.stringify(row)} into big query`);
    } catch (error) {
        logger(`Error occurred while saving user flag: ${JSON.stringify(row)}  to big query. 
        Error Message: ${JSON.stringify(error.message)}. Error stack: ${JSON.stringify(error.stack)}`);
    }
};

const logFraudulentUserFlag = async (userAccountInfo, ruleLabel, reasonForFlaggingUser) => {
    logger(`Save new fraudulent flag for user with info: ${JSON.stringify(userAccountInfo)} for reason: '${reasonForFlaggingUser}'`);
    const row = constructPayloadForUserFlagTable(userAccountInfo, reasonForFlaggingUser);

    await insertUserFlagIntoTable(row);
};

const constructNotificationPayload = (userAccountInfo, reasonForFlaggingUser) => ({
        notificationType: EMAIL_TYPE,
        contacts: CONTACTS_TO_BE_NOTIFIED,
        message: `User: ${userAccountInfo.userId} with account: ${userAccountInfo.accountId} has been flagged as fraudulent. Reason for flagging User: ${reasonForFlaggingUser}`,
        subject: EMAIL_SUBJECT_FOR_ADMINS
    });

const logFraudulentUserAndNotifyAdmins = async (event, userAccountInfo) => {
    logger(`Processing success result of rules engines with event: ${JSON.stringify(event)}`);
    logger(`log fraudulent user and notify admins. User Info: ${JSON.stringify(userAccountInfo)}`);
    // 'results' is an object containing successful events, and an Almanac instance containing facts
    const { ruleLabel, reasonForFlaggingUser } = event.params;
    
    await logFraudulentUserFlag(userAccountInfo, ruleLabel, reasonForFlaggingUser);
        
    const notificationPayload = constructNotificationPayload(userAccountInfo, reasonForFlaggingUser);

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
        const formattedRulesWithLatestFlagTime = await obtainLastAlertTimesForUser(CUSTOM_RULES, userId);

        const extraConfig = {
            url: FETCH_USER_BEHAVIOUR_URL,
            body: {
                userId,
                accountId,
                ruleCutOffTimes: formattedRulesWithLatestFlagTime
            },
            method: POST
        };

        const response = await sendHttpRequest(extraConfig, FETCH_USER_BEHAVIOUR);
        logger(`Successfully fetched facts from user behaviour. Facts: ${JSON.stringify(response.body)}`);

        if (!response || !response.body || response.statusCode !== httpStatus.OK) {
            throw new Error('Could not fetch facts from user behaviour');
        }

        return response.body;
    } catch (error) {
        logger(`Error occurred while fetching facts from user behaviour service for user id: ${userId}. 
        Error: ${JSON.stringify(error.message)}`);
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
    const message = requestStatus.info
        ? `${baseMessage}. Extra info: ${JSON.stringify(requestStatus.info)}` : baseMessage;
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
        throw error;
    }
};


const fetchFactsAboutUserAndRunEngine = async (req, res) => {
    sendSuccessResponse(req, res); // so that function that triggers this can exit successfully
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
        logger(`Error occurred while fetching facts about user and running engine with facts/rules. Error: ${error.message}`);

        const info = {
            errorMessage: error.message,
            errorStack: error.stack
        };
        await sendNotificationOfResultToAdmins({ result: 'FAILED', info }).
            catch((err) => logger(`Error while sending failure of fraud detector to run properly. Error: ${JSON.stringify(err)}`));
    }
};

module.exports = {
    logFraudulentUserAndNotifyAdmins,
    logFraudulentUserFlag,
    createEngineAndRunFactsAgainstRules,
    constructNotificationPayload,
    notifyAdminsOfNewlyFlaggedUser,
    constructPayloadForUserFlagTable,
    insertUserFlagIntoTable,
    fetchFactsAboutUserAndRunEngine,
    sendNotificationOfResultToAdmins,
    isRuleSafeOrIsExperimentalModeOn
};
