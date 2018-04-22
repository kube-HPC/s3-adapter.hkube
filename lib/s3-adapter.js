
const S3Client = require('@hkube/s3-client');
const Components = {
    S3: 'S3Adapter'
};

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

    async put(options) {
        const start = Date.now();
        const result = await S3Client.put({ Bucket: options.jobId, Key: options.taskId, Body: options.data });
        const end = Date.now();
        if (this._log)
            this._log.debug(`Execution of put takes ${end - start}ms`, { component: Components.S3 });

        return { Key: result.Key, Bucket: result.Bucket }
    }

    async get(options) {
        const start = Date.now();
        const res = await S3Client.get(options);
        const end = Date.now();

        if (this._log)
            this._log.debug(`Execution of get takes ${end - start}ms`, { component: Components.S3 });

        return res
    }

    async jobPath(options) {
        const start = Date.now();
        const res = await S3Client.createBucket({ Bucket: options.jobId });
        const end = Date.now();
        if (this._log)
            this._log.debug(`Execution of createBucket takes ${end - start}ms`, { component: Components.S3 });

        return res;
    }

    async getStream(options) {
        // NOT IN USE
        return S3Client.getStream(options);
    }
}

module.exports = new S3Adapter();
