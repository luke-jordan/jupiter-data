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
        event: { // define the event to fire when the conditions evaluate truthy
            type: 'flaggedAsFraudulent',
            params: {
                reasonForFlaggingUser: `User has deposited 50,000 rands 3 or more times in the last 6 months`
            }
        }
    }]
};

// TODO: define rules to match Avish's algorithm
