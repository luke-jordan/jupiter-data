'use strict';

module.exports = {
    allRules: [
       {
            conditions: {
                any: [
                    {
                        fact: 'countOfDepositsGreaterThanHundredThousand',
                        operator: 'greaterThan',
                        value: 0
                    }
                ]
            },
            event: { // define the event to fire when the conditions evaluate truthy
                type: 'flaggedAsFraudulent',
                params: {
                    reasonForFlaggingUser: `User has a deposit greater than 100,000 rands`
                }
            }
        },
        {
            conditions: {
                any: [
                    {
                        fact: 'countOfDepositsGreaterThanBenchmarkWithinSixMonthPeriod',
                        operator: 'greaterThanInclusive',
                        value: 3
                    }
                ]
            },
            event: {
                type: 'flaggedAsFraudulent',
                params: {
                    reasonForFlaggingUser: `User has deposited 50,000 rands 3 or more times in the last 6 months`
                }
            }
        },
        {
            conditions: {
                any: [
                    {
                        fact: 'latestDeposit',
                        operator: 'greaterThan',
                        value: {
                            fact: 'sixMonthAverageDepositMultipliedByN'
                        }
                    }
                ]
            },
            event: {
                type: 'flaggedAsFraudulent',
                params: {
                    reasonForFlaggingUser: `User's latest inward transfer > 10x past 6 month average transfer`
                }
            }
        },
        {
            conditions: {
                any: [
                    {
                        fact: 'countOfWithdrawalsWithin48HoursOfDepositDuringA30DayCycle',
                        operator: 'greaterThan',
                        value: 3
                    }
                ]
            },
            event: {
                type: 'flaggedAsFraudulent',
                params: {
                    reasonForFlaggingUser: `User has more than 3 instances within a 30 day cycle, of withdrawal of deposit within 48 hours of depositing`
                }
            }
        },
        {
            conditions: {
                any: [
                    {
                        fact: 'countOfWithdrawalsWithin24HoursOfDepositDuringA7DayCycle',
                        operator: 'greaterThan',
                        value: 1
                    }
                ]
            },
            event: {
                type: 'flaggedAsFraudulent',
                params: {
                    reasonForFlaggingUser: `User has more than 1 instance within a 7 day cycle, of withdrawal instruction of deposit transferred within 24 hours`
                }
            }
        }
    ]
};
