
const S3Client = require('@hkube/s3-client');
const MODULE_NAME = '@hkube/s3-adapter';

class S3Adapter {
    constructor() {
        this._wasInit = false;
    }

    async init(connection) {
        if (!_wasInit) {
            S3Client.init(connection);
            _wasInit = true;
        }
    }

    async put(options) {
        await S3Client.put({ Bucket: options.pipelineId, Key: options.taskId, Body: options.data });
        return { Bucket: options.pipelineId, Key: options.taskId, moduleName: MODULE_NAME }
    }

    async putStream(options) {
        await S3Client.putStream({ Bucket: options.pipelineId, Key: options.taskId, Body: options.data });
        return { Bucket: options.pipelineId, Key: options.taskId, moduleName: MODULE_NAME }
    }

    async get(options) {
        return S3Client.get({ Bucket: options.pipelineId, Key: options.taskId });
    }

    async getStream(options) {
        return S3Client.getStream({ Bucket: options.pipelineId, Key: options.taskId });
    }
}

module.exports = new S3Adapter();
