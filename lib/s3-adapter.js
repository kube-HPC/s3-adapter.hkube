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
            this._bucketName = connection.bucketName;
            if (bootstrap) {
                const bucketsToCreate = this._bucketName ? [this._bucketName] : buckets;
                await Promise.all(Object.values(bucketsToCreate).map(bucketName => this.createBucket({
                    Bucket: bucketName,
                    CreateBucketConfiguration: { LocationConstraint: connection.region }
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
        return this._put({ ...this._parsePath(options.path), Body: options.data, Metadata: options.metadata });
    }

    async _put(options) {
        const params = validator.validateAndReplacePutParams(options);
        const result = await this._client.upload(params).promise();
        if (this._bucketName) {
            return { path: result.Key };
        }
        return { path: path.join(result.Bucket, result.Key) };
    }

    async _get(options) {
        const parsedPath = this._parsePath(options.path);
        const params = validator.validateAndReplaceGetParams({ ...parsedPath, Range: options.Range });
        const data = await this._client.getObject(params).promise();
        return data;
    }

    async get(options) {
        const data = await this._get(options);
        return data.Body;
    }

    async getWithMetaData(options) {
        const data = await this._get(options);
        return { data: data.Body, metadata: validator.decodeMetadata(data.Metadata) };
    }

    /**
     * This method initiate a multipart upload and get an upload ID.
     * then it creates for each item a part request with the upload ID.
     * Each part must be at least 5 MB in size, except the last part.
     * Finally, it send a complete Multipart-Upload request.
     */
    async multiPart(option) {
        const options = { ...this._parsePath(option.path), Body: option.data };
        const params = validator.validateAndReplacePutParams(options);
        const multiPartParams = {
            Bucket: params.Bucket,
            Key: params.Key
        };
        const { Body: data } = params;
        const multipart = await this._client.createMultipartUpload(multiPartParams).promise();
        const multipartMap = {
            Parts: []
        };
        let partNum = 0;
        for (const d of data) {  // eslint-disable-line
            partNum += 1;
            const partParams = {
                ...multiPartParams,
                Body: d,
                PartNumber: String(partNum),
                UploadId: multipart.UploadId
            };
            const result = await this._client.uploadPart(partParams).promise(); // eslint-disable-line
            multipartMap.Parts[partNum - 1] = {
                ETag: result.ETag,
                PartNumber: Number(partNum)
            };
        }
        const doneParams = {
            ...multiPartParams,
            MultipartUpload: multipartMap,
            UploadId: multipart.UploadId
        };
        const result = await this._client.completeMultipartUpload(doneParams).promise();
        return { path: path.join(result.Bucket, result.Key) };
    }

    async getMetadata(options) {
        const parsedPath = this._parsePath(options.path);
        const params = validator.validateAndReplaceGetParams(parsedPath);
        const data = await this._client.headObject(params).promise();
        return { size: data.ContentLength, metadata: validator.decodeMetadata(data.Metadata) };
    }

    async getStream(options) {
        const parsedPath = this._parsePath(options.path);
        const Range = await this._createBytesRange(options);
        const params = validator.validateAndReplaceGetParams({ ...parsedPath, Range });
        return this._client.getObject(params).createReadStream();
    }

    async getBuffer(options) {
        const params = validator.validateAndReplaceGetParams(options);
        const data = await this._client.getObject(params).promise();
        return data.Body;
    }

    async seek(options) {
        const Range = await this._createBytesRange(options);
        return this.get({ ...options, Range });
    }

    async _createBytesRange(options) {
        const { start, end } = options;
        if (!start && !end) {
            return undefined;
        }
        return `bytes=${start}-${end - 1}`;
    }

    async list(options) {
        const parsedPath = this._parsePath(options.path);
        const result = await this._listObjects({ Bucket: parsedPath.Bucket, Prefix: parsedPath.Key, MaxKeys: options.maxKeys });
        return result.map(x => ({ path: this._bucketName ? x.Key : path.join(parsedPath.Bucket, x.Key) }));
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
        const maxKeys = options.MaxKeys || Number.MAX_SAFE_INTEGER;

        while (isTruncated && results.length < maxKeys) {
            // eslint-disable-next-line
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
        const result = await this.listObjectsWithStats({ Bucket: parsedPath.Bucket, Prefix: parsedPath.Key, MaxKeys: options.maxKeys });
        return result.map(x => ({ path: this._bucketName ? x.Key : path.join(parsedPath.Bucket, x.Key), size: x.Size, mtime: x.Mtime.toISOString() }));
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
        if (this._bucketName) {
            return { Bucket: this._bucketName, Key: fullPath };
        }
        const seperatedPath = fullPath.split(path.sep);
        const Bucket = seperatedPath[0];
        const key = seperatedPath.slice(1, seperatedPath.length);
        return { Bucket, Key: key.join(path.sep) };
    }

    async putStream(options) {
        return this._put({ ...this._parsePath(options.path), Body: options.data, Metadata: options.metadata });
    }

    async getAbsolutePath(relativePath) {
        if (this._bucketName) {
            return `s3://${this._bucketName}${path.sep}${relativePath}`;
        }
        return `s3://${relativePath}`;
    }
}

module.exports = S3Adapter;
