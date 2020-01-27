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
            this._client = new S3Client();
            this._client.init(connectionLocal);
            this._wasInit = true;

            if (bootstrap) {
                await Promise.all(Object.values(buckets).map(bucketName => this._client.createBucket({
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
        const result = await this._client.put(options);
        return { path: path.join(result.Bucket, result.Key) };
    }

    async get(options) {
        const parsedPath = this._parsePath(options.path);
        return this._client.get(parsedPath);
    }

    async list(options) {
        const parsedPath = this._parsePath(options.path);
        const result = await this._client.listObjects({ Bucket: parsedPath.Bucket, Prefix: parsedPath.Key });
        return result.map(x => ({ path: path.join(parsedPath.Bucket, x.Key) }));
    }

    async listWithStats(options) {
        const parsedPath = this._parsePath(options.path);
        const result = await this._client.listObjectsWithStats({ Bucket: parsedPath.Bucket, Prefix: parsedPath.Key });
        return result.map(x => ({ path: path.join(parsedPath.Bucket, x.Key), size: x.Size, mtime: x.Mtime.toISOString() }));
    }

    async listPrefixes(options) {
        const parsedPath = this._parsePath(options.path);
        const res = await this._client.listByDelimiter({ Bucket: parsedPath.Bucket, Delimiter: '/' });
        return res.map(r => r.replace(/\/$/, ''));
    }

    async delete(options) {
        const parsedPath = this._parsePath(options.path);
        const objectsToDelete = await this._client.listObjects({ Bucket: parsedPath.Bucket, Prefix: parsedPath.Key });
        return this._client.deleteObjects({ Bucket: parsedPath.Bucket, Delete: { Objects: objectsToDelete } });
    }

    _parsePath(fullPath) {
        const seperatedPath = fullPath.split(path.sep);
        const Bucket = seperatedPath[0];
        const key = seperatedPath.slice(1, seperatedPath.length);
        return { Bucket, Key: key.join(path.sep) };
    }

    async getStream(options) {
        const parsedPath = this._parsePath(options.path);
        return this._client.getStream(parsedPath);
    }

    async putStream(options) {
        return this._put({ ...this._parsePath(options.path), Body: options.data });
    }

    async getAbsolutePath(relativePath) {
        return `s3://${relativePath}`;
    }
}

module.exports = S3Adapter;
