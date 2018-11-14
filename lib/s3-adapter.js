const https = require('https');
const S3Client = require('@hkube/s3-client');
const path = require('path');
const { BUCKETS_NAMES } = require('../consts/buckets-names');

class S3Adapter {
    constructor() {
        this._wasInit = false;
    }

    async init(connection, log, bootstrap = false) {
        if (!this._wasInit) {
            const connectionLocal = { ...connection };
            if (connectionLocal.endpoint.startsWith('https')) {
                connectionLocal.httpOptions = {
                    agent: new https.Agent({ rejectUnauthorized: false })
                };
            }
            S3Client.init(connectionLocal);
            this._wasInit = true;
            this._log = log;

            if (bootstrap) {
                await Promise.all(Object.values(BUCKETS_NAMES).map(bucketName => S3Client.createBucket({
                    Bucket: bucketName,
                    CreateBucketConfiguration: { LocationConstraint: '' }
                })));
            }
        }
    }

    async put(options) {
        return this._put({ ...this._parsePath(options.Path), Body: options.Data });
    }

    _parsePath(fullPath) {
        const seperatedPath = fullPath.split(path.sep);
        const Bucket = seperatedPath[0];
        const key = seperatedPath.slice(1, seperatedPath.length);
        return { Bucket, Key: key.join(path.sep) };
    }

    async _put(options) {
        const result = await S3Client.put(options);
        return { Key: result.Key, Bucket: result.Bucket };
    }

    async get(options) {
        return S3Client.get(options);
    }
}

module.exports = new S3Adapter();
