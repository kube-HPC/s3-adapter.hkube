
const S3Client = require('@hkube/s3-client');

class S3Adapter {
    constructor() {
        this._client = null;
    }

    async init(connection) {
        if (this._client == null) {
            return S3Client.init(connection);
        }
        return this._client;
    }

    async put(options) {
        return S3Client.put({ Bucket: options.pipelineId, Key: options.taskId, Body: options.data });
    }

    async putStream(options) {
        return S3Client.putStream({ Bucket: options.pipelineId, Key: options.taskId, Body: options.data });
    }

    async get(options) {
        return S3Client.get({ Bucket: options.pipelineId, Key: options.taskId });
    }

    async getStream(options) {
        return S3Client.getStream({ Bucket: options.pipelineId, Key: options.taskId });
    }
}

module.exports = new S3Adapter();
