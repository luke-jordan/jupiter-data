'use strict';

const crypto = require('crypto');

const config = require('config');
const SECRET = config.get('SECRET');
const DIGEST = config.get('DIGEST');
const HASHING_ALGORITHM = config.get('HASHING_ALGORITHM');
const DRY_RUN = config.get('DRY_RUN');
const logger = require('debug')('jupiter:auth-validator');

console.log('testing secret: ', SECRET);

const generateHash = (key) => {
    const hash = crypto.createHmac(HASHING_ALGORITHM, SECRET).
        update(`${SECRET}_${key}`).
        digest(DIGEST);

    logger(`system generated hash: ${hash}`);
    return hash;
};

const verifyHash = (givenHash, systemGeneratedHash) => {
    logger(`comparing given hash: ${givenHash} with system generated hash: ${systemGeneratedHash}`);
    return givenHash === systemGeneratedHash;
};

const authValidator = (givenHash, givenKey) => {
    logger(`Authenticating request with hash: ${givenHash} and key: ${givenKey}`);

    if (DRY_RUN) {
        logger('still testing, skip authentication');
        return;
    }

    if (verifyHash(givenHash, generateHash(givenKey))) {
        logger(`authentication request succeeded`);
        return;
    }

    const errorMessage = 'Authentication Request Failed';
    logger(errorMessage);
    throw new Error(errorMessage);
};

module.exports = authValidator;
