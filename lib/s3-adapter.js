
const S3Client = require('@hkube/s3-client');
const component = 'S3Adapter';

class S3Adapter {
    constructor() {
        this._wasInit = false;
    }

    async init(connection, log) {
        if (!this._wasInit) {
            S3Client.init(connection);
            this._wasInit = true;
            this._log = log;
        }
    }

    async putResults(options) {
        const start = Date.now();
        const result = await S3Client.put({ Bucket: options.jobId, Key: 'result.json', Body: options.data });
        const end = Date.now();
        if (this._log)
            this._log.debug(`Execution of putResult takes ${end - start}ms`, { component });

        return { Key: 'result.json', Bucket: result.Bucket }
    }

    async put(options) {
        const start = Date.now();
        const result = await S3Client.put({ Bucket: options.jobId, Key: options.taskId, Body: options.data });
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

    async getResults(options) {
        const start = Date.now();
        const res = await S3Client.get({ Bucket: options.jobId, Key: 'result.json' });
        const end = Date.now();

        if (this._log)
            this._log.debug(`Execution of getResult takes ${end - start}ms`, { component });

        return res
    }

    async jobPath(options) {
        const start = Date.now();
        const res = await S3Client.createBucket({ Bucket: options.jobId });
        const end = Date.now();
        if (this._log) {
            const diff = end - start;
            this._log.debug(`Execution of jobPath takes ${diff} ms`, { component, operation: 'jobPath', time: diff });
        }

        return res;
    }

    async getStream(options) {
        // NOT IN USE
        return S3Client.getStream(options);
    }
}

module.exports = new S3Adapter();
