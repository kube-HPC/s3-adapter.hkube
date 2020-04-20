const https = require('https');
const AWS = require('aws-sdk');
const path = require('path');
const constants = require('./constants');
const validator = require('./s3-validator-replacer');

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
            const opt = {
                s3BucketEndpoint: false,
                s3ForcePathStyle: true,
                ...connectionLocal
            };
            this._client = new AWS.S3(opt);
            this._wasInit = true;

            if (bootstrap) {
                await Promise.all(Object.values(buckets).map(bucketName => this.createBucket({
                    Bucket: bucketName,
                    CreateBucketConfiguration: { LocationConstraint: '' }
                })));
            }
        }
    }

    async createBucket(options) {
        const bucketParams = validator.validateAndReplaceBucketParams(options);
        return this._createBucketIfNotExists(bucketParams);
    }

    async _isBucketExists(options) {
        try {
            await this._client.headBucket({ Bucket: options.Bucket }).promise();
            return true;
        }
        catch (error) {
            if (error && error.statusCode === constants.NO_SUCH_BUCKET_STATUS_CODE) {
                return false;
            }
            throw error;
        }
    }

    async _createBucketIfNotExists(options) {
        const exists = await this._isBucketExists(options);
        if (!exists) {
            await this._client.createBucket(options).promise();
        }
    }

    async put(options) {
        return this._put({ ...this._parsePath(options.path), Body: options.data });
    }

    async _put(options) {
        const params = validator.validateAndReplacePutParams(options);
        const result = await this._client.upload(params).promise();
        return { path: path.join(result.Bucket, result.Key) };
    }

    async get(options) {
        const parsedPath = this._parsePath(options.path);
        const params = validator.validateAndReplaceGetParams(parsedPath);
        const data = await this._client.getObject(params).promise();
        return data.Body;
    }

    async getMetadata(options) {
        const parsedPath = this._parsePath(options.path);
        const params = validator.validateAndReplaceGetParams(parsedPath);
        const data = await this._client.headObject(params).promise();
        return { size: data.ContentLength };
    }

    async list(options) {
        const parsedPath = this._parsePath(options.path);
        const result = await this._listObjects({ Bucket: parsedPath.Bucket, Prefix: parsedPath.Key });
        return result.map(x => ({ path: path.join(parsedPath.Bucket, x.Key) }));
    }

    async listObjects(options) {
        const results = await this._listObjects(options);
        return results.map(elem => ({ Key: elem.Key }));
    }

    async listObjectsWithStats(options) {
        const results = await this._listObjects(options);
        return results.map(elem => ({ Key: elem.Key, Size: elem.Size, Mtime: elem.LastModified }));
    }

    async _listObjects(options) {
        let results = [];
        let isTruncated = true;
        let continuationToken = null;

        while (isTruncated) {
            const lastResults = await this._client.listObjectsV2({
                ...options,
                ContinuationToken: continuationToken
            }).promise();
            results = results.concat(lastResults.Contents);
            isTruncated = lastResults.IsTruncated;
            continuationToken = lastResults.NextContinuationToken;
        }
        return results;
    }

    async listWithStats(options) {
        const parsedPath = this._parsePath(options.path);
        const result = await this.listObjectsWithStats({ Bucket: parsedPath.Bucket, Prefix: parsedPath.Key });
        return result.map(x => ({ path: path.join(parsedPath.Bucket, x.Key), size: x.Size, mtime: x.Mtime.toISOString() }));
    }

    async listPrefixes(options) {
        const parsedPath = this._parsePath(options.path);
        const key = parsedPath.Key;
        const prefix = key.endsWith('/') ? key : (key && path.join(key, '/')) || '';
        const res = await this.listByDelimiter({ Bucket: parsedPath.Bucket, Prefix: prefix, Delimiter: '/' });
        return res.map(r => r.replace(prefix, '').replace(/\/$/, ''));
    }

    async listByDelimiter(options) {
        const res = await this._client.listObjectsV2(options).promise();
        const prefixes = res.CommonPrefixes.length > 0 && res.CommonPrefixes.map(x => x.Prefix);
        const result = prefixes || res.Contents.map(x => x.Key);
        return result;
    }

    async deleteObjects(options) {
        const promiseArray = [];
        while (options.Delete.Objects.length) {
            promiseArray.push(this._client.deleteObjects({
                Bucket: options.Bucket,
                Delete: {
                    Objects: options.Delete.Objects.splice(0, 1000)
                }
            }).promise());
        }
        return Promise.all(promiseArray);
    }

    async delete(options) {
        const parsedPath = this._parsePath(options.path);
        const objectsToDelete = await this.listObjects({ Bucket: parsedPath.Bucket, Prefix: parsedPath.Key });
        return this.deleteObjects({ Bucket: parsedPath.Bucket, Delete: { Objects: objectsToDelete } });
    }

    _parsePath(fullPath) {
        const seperatedPath = fullPath.split(path.sep);
        const Bucket = seperatedPath[0];
        const key = seperatedPath.slice(1, seperatedPath.length);
        return { Bucket, Key: key.join(path.sep) };
    }

    async getStream(options) {
        const parsedPath = this._parsePath(options.path);
        const getParams = validator.validateAndReplaceGetParams(parsedPath);
        return this._client.getObject(getParams).createReadStream();
    }

    async getBuffer(options) {
        const getParams = validator.validateAndReplaceGetParams(options);
        const data = await this._client.getObject(getParams).promise();
        return data.Body;
    }

    async putStream(options) {
        return this._put({ ...this._parsePath(options.path), Body: options.data });
    }

    async getAbsolutePath(relativePath) {
        return `s3://${relativePath}`;
    }
}

module.exports = S3Adapter;
