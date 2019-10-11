const chai = require('chai');
const expect = chai.expect;
const authValidator = require('../index');

describe('Testing Auth Validator', () => {
    it('refuses an invalid hash and key combination', () => {
        expect(function () { authValidator('hello', 'world') })
            .to.throw('Authentication Request Failed');
    });

    it('correctly verifies a valid hash and key', () => {
        const result = authValidator(
            'ed33811633da1d13733b3914bf712ee2473964872c012e95312f3483056f5db1',
            'world'
        );
        const expectedResult = undefined;

        expect(result).to.equal(expectedResult);
    });
});