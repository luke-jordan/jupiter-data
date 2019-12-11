'use strict';

const moment = require('moment');

const sinon = require('sinon');

const biqQueryStub = sinon.stub();

const handler = { };

describe('Initial, simplest fall off analysis', () => {

    beforeEach(() => biqQueryStub.reset());

    // understand overall architecture
    // decide between python and js

    // write tests
    // write functions
    it('Selects drop-offs and recoveries from prior day', async () => {
        const testEvent = {
            startStep: 'ENTERED_ONBOARD_SCREEN',
            endSteps: ['ENTERED_REFERRAL_CODE'],
            disqualifyingStep: ['USER_LOGGED_IN']
        };

        const intervalStart = moment().startOf('day').subtract(1, 'day'); // start of yesterday (= start of today minus one day)
        const intervalEnd = moment().startOf('day'); // end of yesterday = beginning of today

        // dropoffs =======>

        const expectedQuery1 = 'select user_id from events_table where creation_time between $start_date and $end_date and event_type = $start_step';
        const expectedQuery2 = 'select distinct(user_id) from events_table where creation_time between $start_date and $end_date and event_type in ($end_steps)';
        const expectedQuery3 = 'select distinct(user_id) from events_table where creation_time < end_date and event_type in ($disqualifyingSteps)';

        // find users who had an onboarding event but not certain events ("referral", "logged_in") between certain dates

        // 1. select users that have all considered events: ["onboarding", "referral", "logged_in"]
        // 2. selects users that have events we want to filter out: ["referral", "logged_in"]
        // loop through all users in 2 if found in 1, delete row => thus the userId

        // select userId from events_table where userId not in (
        // 	select userId
        // 	from events_table
        // 	where event_type in ("referral", "logged_in") and creation_time <= $end_date
        // ) and event_type = "onboarding"
        // and creation_time >= $start_date and creation_time <= $end_date


        const enteredStartStepYesterdaySet = [];
        const reachedEndStepsYesterdaySet = [];
        const reachedDisqualifyingStepInPastSet = [];

        biqQueryStub.withArgs(expectedQuery1).resolves(enteredStartStepYesterdaySet);
        bigQueryStub.withArgs(expectedQuery2).resolves(reachedEndStepsYesterdaySet);
        bigQueryStub.withArgs(expectedQuery3).resolves(reachedDisqualifyingStepInPastSet);

        const expectedDropOffs = enteredStartStepYesterdaySet - (reachedEndStepsYesterdaySet + reachedDisqualifyingStepInPastSet);

        // recoveries ====>
        // select users with onboarding event before yesterday but no referral screen before yesterday (between certain dates), who now entered a referral screen yesterday

        // first part: select users with onboarding event before yesterday but no referral screen before yesterday (or between certain dates)
        // select userId from events_table where userId not in
        // (
        // 	select userId
        // 	from events_table
        // 	where event_type in ("referral", "logged_in") and creation_time <= $yesterday
        // )
        // and event_type = "onboarding"
        // and creation_time >= $start_date and creation_time <= $yesterday

        // second part: select users who entered a referral screen yesterday
        // select user_id from events_table
        // where event_type="referral"
        // and creation_time >= $yesterday (between yesterday_start and yesterday_end)

        // full part:
        // select user_id from events_table
        // where event_type="referral"
        // and creation_time >= $yesterday (between yesterday_start and yesterday_end)
        // and userId in
        // (
            // select userId from events_table where userId not in
                // (
                // 	select userId
                // 	from events_table
                // 	where event_type in ("referral", "logged_in") and creation_time <= $yesterday
                // )
            // and event_type = "onboarding"
            // and creation_time >= $end_date and creation_time <= $yesterday
        // )

        const enteredStartPreYestQuery = 'select distinct(user_id) from events_table where creation_time before $start_date and event_type = $start_step';
        const reachedEndStepPreYestQuery = 'select distinct(user_id) from events_table where creation_time before $start_date and event_type in ($end_steps)';

        const enteredStartStepBeforeYesterdaySet = [];
        const reachedEndStepPreYesterdaySet = [];

        const reachedEndStepYesterdayNotBeforeSet = (reachedEndStepsYesterdaySet - reachedEndStepPreYesterdaySet);
        const expectedRecoveries = enteredStartStepBeforeYesterdaySet *** reachedEndStepYesterdayNotBeforeSet; // *** = intersection

        bigQueryStub.withArgs(enteredStartPreYestQuery).resolves(enteredStartStepBeforeYesterdaySet);
        bigQueryStub.withArgs(reachedEndStepPreYestQuery).resolves(reachedEndStepPreYesterdaySet);

        const result = await handler.countDropOffsAndRecoveries(testEvent);

        expect(result).to.deep.equal({ dropoffs: expectedDropOffs, recoveries: expectedRecoveries });
    });

});