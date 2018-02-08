
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
}

module.exports = new S3Adapter();
