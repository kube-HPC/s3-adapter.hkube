const moment = require('moment');
const S3Client = require('@hkube/s3-client');
const { BUCKETS_NAMES } = require('../consts/buckets-names');
const component = 'S3Adapter';
const DATE_FORMAT = "DD-MM-YYYY";

class S3Adapter {
    constructor() {
        this._wasInit = false;
    }

    async init(connection, log) {
        if (!this._wasInit) {
            S3Client.init(connection);
            this._wasInit = true;
            this._log = log;
            Object.values(BUCKETS_NAMES).forEach((bucketName) => {
                await S3Client.createBucket({ Bucket: bucketName });
            });
        }
    }

    async putResults(options) {
        return this._put({ Bucket: BUCKETS_NAMES.HKUBE_RESULTS, Key: `${moment().format(DATE_FORMAT)}/${options.jobId}/result.json`, Body: options.data });
    }

    async put(options) {
        return this._put({ Bucket: BUCKETS_NAMES.HKUBE, Key: `${moment().format(DATE_FORMAT)}/${options.jobId}/${options.taskId}`, Body: options.data });
    }

    async _put(options) {
        const start = Date.now();
        const result = await S3Client.put(options);
        const end = Date.now();
        if (this._log) {
            const diff = end - start;
            this._log.debug(`Execution of put takes ${diff} ms`, { component, operation: 'put', time: diff });
        }

        return { Key: result.Key, Bucket: result.Bucket }
    }

    async get(options) {
        const start = Date.now();
        const res = await S3Client.get(options);
        const end = Date.now();

        if (this._log) {
            const diff = end - start;
            this._log.debug(`Execution of get takes ${diff} ms`, { component, operation: 'get', time: diff });
        }

        return res
    }

    async jobPath(options) {//todo - not in use
    }

    async getStream(options) {
        // NOT IN USE
        return S3Client.getStream(options);
    }
}

module.exports = new S3Adapter();
