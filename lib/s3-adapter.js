
const S3Client = require('@hkube/s3-client');

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
        const result = await S3Client.put({ Bucket: options.jobId, Key: options.taskId, Body: options.data });
        return { Key: result.Key, Bucket: result.Bucket }
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
