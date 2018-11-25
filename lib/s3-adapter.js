const https = require('https');
const S3Client = require('@hkube/s3-client');
const path = require('path');

class S3Adapter {
    constructor() {
        this._wasInit = false;
    }

    async init(connection, buckets, bootstrap = false) {
        if (!this._wasInit) {
            const connectionLocal = { ...connection };
            if (connectionLocal.endpoint.startsWith('https')) {
                connectionLocal.httpOptions = {
                    agent: new https.Agent({ rejectUnauthorized: false })
                };
            }
            S3Client.init(connectionLocal);
            this._wasInit = true;

            if (bootstrap) {
                await Promise.all(Object.values(buckets).map(bucketName => S3Client.createBucket({
                    Bucket: bucketName,
                    CreateBucketConfiguration: { LocationConstraint: '' }
                })));
            }
        }
    }

    async put(options) {
        return this._put({ ...this._parsePath(options.path), Body: options.data });
    }

    async _put(options) {
        const result = await S3Client.put(options);
        return { path: path.join(result.Bucket, result.Key) };
    }

    async get(options) {
        const parsedPath = this._parsePath(options.path);
        return S3Client.get(parsedPath);
    }

    async list(options) {
        const parsedPath = this._parsePath(options.path);
        return this._listObjects({ Bucket: parsedPath.Bucket, Prefix: parsedPath.Key });
    }

    async _listObjects(options) {
        const res = await S3Client.listObjects({ Bucket: options.Bucket, Prefix: options.Prefix });
        return res.map((x) => {
            return { path: path.join(options.Bucket, x.Key) };
        });
    }

    async delete(options) {
        const parsedPath = this._parsePath(options.path);
        const objectsToDelete = await S3Client.listObjects({ Bucket: parsedPath.Bucket, Prefix: parsedPath.Key });
        return S3Client.deleteObjects({ Bucket: parsedPath.Bucket, Delete: { Objects: objectsToDelete } });
    }

    _parsePath(fullPath) {
        const seperatedPath = fullPath.split(path.sep);
        const Bucket = seperatedPath[0];
        const key = seperatedPath.slice(1, seperatedPath.length);
        return { Bucket, Key: key.join(path.sep) };
    }
}

module.exports = new S3Adapter();
