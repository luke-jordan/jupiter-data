const crypto = require('crypto');

const {
    SECRET,
    DIGEST,
    HASHING_ALGORITHM
} = require('./constants');

function generateHash(key) {
    const hash = crypto.createHmac(HASHING_ALGORITHM, SECRET)
        .update(`${SECRET}_${key}`)
        .digest(DIGEST);

    console.log(`system generated hash: ${hash}`);
    return hash;
}

function verifyHash(givenHash, systemGeneratedHash) {
    console.log(`comparing given hash: ${givenHash} with system generated hash: ${systemGeneratedHash}`)
    return givenHash === systemGeneratedHash;
}

function authValidator(givenHash, givenKey) {
    console.log(`authenticating request with hash: ${givenHash} and key: ${givenKey}`);

    if (verifyHash(givenHash, generateHash(givenKey))) {
        console.log(`authentication request succeeded`);
        return;
    }

    const errorMessage = 'Authentication Request Failed';
    console.error(errorMessage);
    throw new Error(errorMessage);
}

module.exports = authValidator;