
const S3Client = require('@hkube/s3-client');
const MODULE_NAME = '@hkube/s3-adapter';

class S3Adapter {
    constructor() {
        this._wasInit = false;
    }

    async init(connection) {
        if (!this._wasInit) {
            S3Client.init(connection);
            this._wasInit = true;
        }
    }

    async put(options) {
        let result = await S3Client.put({ Bucket: options.jobId, Key: options.taskId, Body: options.data });
        return Object.assign(result, { moduleName: MODULE_NAME });
    }

    async get(options) {
        return S3Client.get(options);
    }

    async jobPath(options) {
        return S3Client.createBucket({ Bucket: options.jobId });
    }

    async getStream(options) {
        return S3Client.getStream(options);
    }
}

module.exports = new S3Adapter();
