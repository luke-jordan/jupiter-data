'use strict';

module.exports = {
    allRules: [
       {
           conditions: {
                any: [
                    {
                        fact: 'countOfSavingEventsGreaterThanHundredThousand',
                        operator: 'greaterThan',
                        value: 0
                    }
                ]
            },
            event: {
                type: 'flaggedAsFraudulent',
                params: {
                    ruleLabel: `single_very_large_saving_event`,
                    reasonForFlaggingUser: `User has a saving_event greater than 100,000 rands`
                }
            }
        },
        {
            conditions: {
                any: [
                    {
                        fact: 'countOfSavingEventsGreaterThanBenchmarkWithinSixMonthPeriod',
                        operator: 'greaterThanInclusive',
                        value: 3
                    }
                ]
            },
            event: {
                type: 'flaggedAsFraudulent',
                params: {
                    ruleLabel: `saving_events_greater_than_benchmark_within_six_months`,
                    reasonForFlaggingUser: `User has saving_event 50,000 rands 3 or more times in the last 6 months`
                }
            }
        },
        {
            conditions: {
                any: [
                    {
                        fact: 'latestSavingEvent',
                        operator: 'greaterThan',
                        value: {
                            fact: 'sixMonthAverageSavingEventMultipliedByN'
                        }
                    }
                ]
            },
            event: {
                type: 'flaggedAsFraudulent',
                params: {
                    ruleLabel: `latest_saving_event_greater_than_six_months_average`,
                    reasonForFlaggingUser: `User's latest inward transfer > 10x past 6 month average transfer`
                }
            }
        },
        {
            experimental: false,
            conditions: {
                any: [
                    {
                        fact: 'countOfWithdrawalsWithin48HoursOfSavingEventDuringA30DayCycle',
                        operator: 'greaterThan',
                        value: 3
                    }
                ]
            },
            event: {
                type: 'flaggedAsFraudulent',
                params: {
                    ruleLabel: `withdrawals_within_two_days_of_saving_events_during_one_month`,
                    reasonForFlaggingUser: `User has more than 3 instances within a 30 day cycle, of withdrawal of saving_event within 48 hours of saving_eventing`
                }
            }
        },
        {
            experimental: false,
            conditions: {
                any: [
                    {
                        fact: 'countOfWithdrawalsWithin24HoursOfSavingEventDuringA7DayCycle',
                        operator: 'greaterThan',
                        value: 1
                    }
                ]
            },
            event: {
                type: 'flaggedAsFraudulent',
                params: {
                    ruleLabel: `withdrawals_within_one_day_of_saving_events_during_one_week`,
                    reasonForFlaggingUser: `User has more than 1 instance within a 7 day cycle, of withdrawal instruction of saving_event transferred within 24 hours`
                }
            }
        }
    ]
};
