const Validator = require('ajv');
const { putSchema, getSchema, createSchema } = require('./schemas');
const validatorInstance = new Validator({ useDefaults: true, coerceTypes: false });
class S3ValidatorReplacer {
    constructor() {
        this._putSchema = validatorInstance.compile(putSchema);
        this._getSchema = validatorInstance.compile(getSchema);
        this._createSchema = validatorInstance.compile(createSchema);
    }

    validateAndReplaceBucketParams(params) {
        this._validateSchema(this._createSchema, params);
        return { ...params, Bucket: this._parseBucket(params.Bucket) };
    }

    validateAndReplacePutParams(params) {
        this._validateSchema(this._putSchema, params);
        return {
            ...params,
            Bucket: this._parseBucket(params.Bucket),
            Key: this._parseKey(params.Key),
            Body: params.Body,
            Metadata: this.encodeMetadata(params.Metadata)
        };
    }

    validateAndReplaceGetParams(params) {
        this._validateSchema(this._getSchema, params);
        return {
            Bucket: this._parseBucket(params.Bucket),
            Key: this._parseKey(params.Key),
            Range: params.Range,
        };
    }

    _validateSchema(validator, object) {
        const valid = validator(object);
        if (!valid) {
            const error = validatorInstance.errorsText(validator.errors);
            throw new Error(error);
        }
    }

    _parseBucket(bucketName) {
        if (!bucketName.trim()) {
            throw new TypeError('Bucket cannot be empty');
        }
        if (bucketName.length > 63 || bucketName.length < 3) {
            throw new Error('Bucket names must be at least 3 and no more than 63 characters long.');
        }
        return bucketName;
    }

    _parseKey(key) {
        if (!key.trim()) {
            throw new Error('Key cannot be empty');
        }
        return key;
    }

    /**
     * This method get an object of metadata <string, any> and returns new
     * object with the same keys but with encoded base64 values
     * example:
     *  input:  { header: buffer [1,2,3,4], custom: {a: 1, b: 2} };
        output: { header: aLCJiIjo0fQ1==,   custom: json:aLCJiIjo0fQ1== };
     */
    encodeMetadata(metadata) {
        let newMetadata;
        if (metadata) {
            newMetadata = Object.entries(metadata).reduce((acc, cur) => {
                const [k, v] = cur;
                let value;
                if (!Buffer.isBuffer(v)) {
                    const json = JSON.stringify(v);
                    const base64 = Buffer.from(json, 'utf-8').toString('base64');
                    value = `json:${base64}`;
                }
                else {
                    value = v.toString('base64');
                }
                acc[k] = value;
                return acc;
            }, {});
        }
        return newMetadata;
    }

    /**
     * This method get an object of metadata <string, any> and returns new
     * object with the same keys but with decoded base64 values
     * example:
     *  input:  { header: aLCJiIjo0fQ1==,   custom: json:aLCJiIjo0fQ1== };
        output: { header: buffer [1,2,3,4], custom: {a: 1, b: 2} };
     */
    decodeMetadata(metadata) {
        let newMetadata;
        if (metadata) {
            newMetadata = Object.entries(metadata).reduce((acc, cur) => {
                const [k, v] = cur;
                const [type, base64] = v.split(':');
                let value;
                if (type === 'json') {
                    const json = Buffer.from(base64, 'base64').toString('utf-8');
                    value = JSON.parse(json);
                }
                else {
                    value = Buffer.from(v, 'base64');
                }
                acc[k] = value;
                return acc;
            }, {});
        }
        return newMetadata;
    }
}

module.exports = new S3ValidatorReplacer();
