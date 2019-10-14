const crypto = require('crypto');

const config = require('config');
const SECRET = config.get('SECRET');
const DIGEST = config.get('DIGEST');
const HASHING_ALGORITHM = config.get('HASHING_ALGORITHM');

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

exports.authValidator  = function authValidator(givenHash, givenKey) {
    console.log(`authenticating request with hash: ${givenHash} and key: ${givenKey}`);

    if (verifyHash(givenHash, generateHash(givenKey))) {
        console.log(`authentication request succeeded`);
        return;
    }

    const errorMessage = 'Authentication Request Failed';
    console.error(errorMessage);
    throw new Error(errorMessage);
};
