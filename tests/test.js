const chai = require('chai');
const chaiAsPromised = require('chai-as-promised');
chai.use(chaiAsPromised);
const { expect } = chai;
const path = require('path');
const fs = require('fs');
const moment = require('moment');
const uniqid = require('uuid');
const stream = require('stream');
const { EncodingTypes, Encoding } = require('@hkube/encoding');
const S3Adapter = require('../lib/s3-adapter');
const BUCKETS_NAMES = {
    HKUBE: 'hkube',
    HKUBE_RESULTS: 'hkube-results',
    HKUBE_METADATA: 'hkube-metadata',
    HKUBE_STORE: 'hkube-store',
    HKUBE_EXECUTION: 'hkube-execution',
    HKUBE_INDEX: 'hkube-index'
};
const DateFormat = 'YYYY-MM-DD';
let adapter = new S3Adapter();

const options = {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID || 'AKIAIOSFODNN7EXAMPLE',
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    endpoint: process.env.AWS_ENDPOINT || 'http://127.0.0.1:9000'
};

const mock = {
    mocha: '^5.0.0',
    chai: '^4.1.2',
    coveralls: '^3.0.0',
    eslint: '^4.15.0',
    istanbul: '^1.1.0-alpha.1',
    sinon: '^4.1.3'
};
const createJobId = () => uniqid() + '-ab-cd-ef';

describe(`s3-adapter`, () => {
    before(async () => {
        await adapter.init(options, BUCKETS_NAMES, true);
    });

    EncodingTypes.forEach((o) => {
        describe(`s3-adapter ${o}`, () => {
            before(async () => {
                adapter = new S3Adapter();
                await adapter.init(options, BUCKETS_NAMES, false);

                const encoding = new Encoding({ type: o });

                const wrapperGet = (fn) => {
                    const wrapper = async (args) => {
                        let result;
                        if (args.Bucket) {
                            const options = {
                                path: `${args.Bucket}/${args.Key}`,
                            }
                            result = await fn(options);
                        }
                        else {
                            result = await fn(args);
                        }
                        return (!args.ignoreEncode && encoding.decode(result)) || result
                    };
                    return wrapper;
                };

                const wrapperPut = (fn) => {
                    const wrapper = (args) => {
                        if (args.Body) {
                            const Body = (!args.ignoreEncode && encoding.encode(args.Body)) || args.Body;
                            const options = {
                                path: `${args.Bucket}/${args.Key}`,
                                data: Body
                            }
                            return fn(options);
                        }
                        else {
                            const data = (!args.ignoreEncode && encoding.encode(args.data)) || args.data;
                            return fn({ ...args, data });
                        }
                    };
                    return wrapper;
                };
                adapter.put = wrapperPut(adapter.put.bind(adapter));
                adapter.get = wrapperGet(adapter.get.bind(adapter));
            });
            describe('put', () => {
                it(`put result`, async () => {
                    const link = await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE_RESULTS, o, moment().format(DateFormat), 'job-id', 'result.json'), data: 'test' });
                    const res = await adapter.get(link);
                    expect(res).to.equal('test');
                });
                it(`get all tasks of specific jobid`, async () => {
                    const jobId = Date.now();
                    const results = await Promise.all([
                        adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, o, `${moment().format(DateFormat)}/${jobId}/0`), data: 'test0' }),
                        adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, o, `${moment().format(DateFormat)}/${jobId}/1`), data: 'test1' }),
                        adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, o, `${moment().format(DateFormat)}/${jobId}/2`), data: 'test2' }),
                        adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, o, `${moment().format(DateFormat)}/${jobId}/3`), data: 'test3' }),
                        adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, o, `${moment().format(DateFormat)}/${jobId}/4`), data: 'test4' }),
                        adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, o, `${moment().format(DateFormat)}/${jobId}/5`), data: 'test5' }),
                        adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, o, `${moment().format(DateFormat)}/${jobId}/6`), data: 'test6' })]);

                    const res = await adapter.list({ path: path.join(BUCKETS_NAMES.HKUBE, o, moment().format(DateFormat), jobId.toString()) });
                    expect(res.length).to.equal(7);

                    for (let i = 0; i < results.length; i += 1) {
                        const r = await adapter.get(results[i]);
                        expect(r).to.equal('test' + i);
                    }
                });
                it(`get more than 100 items`, async () => {
                    const promiseArray = [];
                    for (let i = 0; i < 100; i += 1) {
                        promiseArray.push(adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, o, o, moment().format(DateFormat), 'more-than-100-keys', 'task' + i), data: `test${i}` }));
                    }
                    await Promise.all(promiseArray);
                    const res = await adapter.list({ path: path.join(BUCKETS_NAMES.HKUBE, o, o, moment().format(DateFormat), 'more-than-100-keys') });
                    expect(res.length).to.equal(100);
                });
                it(`delete more than 100 items`, async () => {
                    const key = 'more-than-100-keys2';
                    const promiseArray = [];
                    for (let i = 0; i < 100; i += 1) {
                        promiseArray.push(adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, o, moment().format(DateFormat), key, 'task' + i), data: `test${i}` }));
                    }
                    await Promise.all(promiseArray);
                    const res = await adapter.list({ path: path.join(BUCKETS_NAMES.HKUBE, o, moment().format(DateFormat), key) });
                    expect(res.length).to.equal(100);

                    await adapter.delete({ path: path.join(BUCKETS_NAMES.HKUBE, o, moment().format(DateFormat), key) });
                    const res2 = await adapter.list({ path: path.join(BUCKETS_NAMES.HKUBE, o, moment().format(DateFormat), key) });
                    expect(res2.length).to.equal(0);
                });
                it(`delete by date`, async () => {
                    await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, o, moment('2015-01-14').format(DateFormat), 'test1', 'test1.json'), data: { data: 'sss' } });
                    await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, o, moment('2015-01-14').format(DateFormat), 'test2', 'test2.json'), data: { data: 'sss' } });
                    await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, o, moment('2015-01-14').format(DateFormat), 'test3', 'test3.json'), data: { data: 'sss' } });
                    await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, o, moment('2015-01-14').format(DateFormat), 'test4', 'test4.json'), data: { data: 'sss' } });
                    await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, o, moment('2015-01-14').format(DateFormat), 'test1', 'test2.json'), data: { data: 'sss' } });
                    await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, o, moment('2015-01-14').format(DateFormat), 'test2', 'test3.json'), data: { data: 'sss' } });
                    await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, o, moment('2015-01-14').format(DateFormat), 'test3', 'test4.json'), data: { data: 'sss' } });
                    await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, o, moment('2015-01-14').format(DateFormat), 'test3', 'test5.json'), data: { data: 'sss' } });
                    await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, o, moment('2015-01-14').format(DateFormat), 'test3', 'test6.json'), data: { data: 'sss' } });
                    await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, o, moment('2015-01-14').format(DateFormat), 'test3', 'test7.json'), data: { data: 'sss' } });

                    await adapter.delete({ path: path.join(BUCKETS_NAMES.HKUBE, o, '2015-01-14') });
                    const res0 = await adapter.list({ path: path.join(BUCKETS_NAMES.HKUBE, o, '2015-01-14') });
                    expect(res0.length).to.equal(0);

                    await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, o, moment('2015-01-14').format(DateFormat), 'test1', 'test1.json'), data: { data: 'sss' } });
                    await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, o, moment('2015-01-14').format(DateFormat), 'test2', 'test2.json'), data: { data: 'sss' } });
                    await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, o, moment('2015-01-14').format(DateFormat), 'test3', 'test3.json'), data: { data: 'sss' } });
                    await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, o, moment('2015-01-14').format(DateFormat), 'test4', 'test4.json'), data: { data: 'sss' } });
                    await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, o, moment('2015-01-14').format(DateFormat), 'test1', 'test2.json'), data: { data: 'sss' } });
                    await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, o, moment('2015-01-14').format(DateFormat), 'test2', 'test3.json'), data: { data: 'sss' } });
                    await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, o, moment('2015-01-14').format(DateFormat), 'test3', 'test4.json'), data: { data: 'sss' } });
                    await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, o, moment('2015-01-14').format(DateFormat), 'test3', 'test5.json'), data: { data: 'sss' } });
                    await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, o, moment('2015-01-14').format(DateFormat), 'test3', 'test6.json'), data: { data: 'sss' } });
                    await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, o, moment('2015-01-14').format(DateFormat), 'test3', 'test7.json'), data: { data: 'sss' } });

                    await adapter.delete({ path: path.join(BUCKETS_NAMES.HKUBE, o, '2015-01-14/test1') });
                    const res1 = await adapter.list({ path: path.join(BUCKETS_NAMES.HKUBE, o, '2015-01-14/test1') });
                    expect(res1.length).to.equal(0);

                    await adapter.delete({ path: path.join(BUCKETS_NAMES.HKUBE, o, '2015-01-14/test2') });
                    const res2 = await adapter.list({ path: path.join(BUCKETS_NAMES.HKUBE, o, '2015-01-14/test2') });
                    expect(res2.length).to.equal(0);

                    await adapter.delete({ path: path.join(BUCKETS_NAMES.HKUBE, o, '2015-01-14/test3') });
                    const res3 = await adapter.list({ path: path.join(BUCKETS_NAMES.HKUBE, o, '2015-01-14/test3') });
                    expect(res3.length).to.equal(0);
                });
                it(`delete by date more than 100 items`, async () => {
                    const promiseArray = [];
                    for (let i = 0; i < 100; i += 1) {
                        promiseArray.push(adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, o, moment('2014-11-28').format(DateFormat), 'test3', `test${i}.json`), data: { data: 'sss' } }));
                    }
                    await Promise.all(promiseArray);
                    await adapter.delete({ path: path.join(BUCKETS_NAMES.HKUBE, o, '2014-11-28') });
                    const res2 = await adapter.list({ path: path.join(BUCKETS_NAMES.HKUBE, o, '2014-11-28') });
                    expect(res2.length).to.equal(0);
                });
                it(`list objects without prefix`, async () => {
                    const jobId = Date.now().toString();
                    await Promise.all([
                        adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, o, moment().format(DateFormat), jobId, '0'), data: 'test0' }),
                        adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, o, moment().format(DateFormat), jobId, '0'), data: 'test0' }),
                        adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE_RESULTS, o, moment().format(this.DateFormat), jobId, 'result.json'), data: 'test0' }),
                        adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE_RESULTS, o, moment().format(this.DateFormat), jobId, 'result.json'), data: 'test6' })]);
                    const res1 = await adapter.list({ path: path.join(BUCKETS_NAMES.HKUBE, o, '/') });
                    expect(res1.length > 0).to.be.true;
                    const res2 = await adapter.list({ path: path.join(BUCKETS_NAMES.HKUBE_RESULTS, o, '/') });
                    expect(res2.length > 0).to.be.true;
                });
                it(`list objects with delimiter`, async () => {
                    const jobId = Date.now().toString();
                    await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE_INDEX, '2019-01-01', jobId, '0'), data: { data: 'sss1' } });
                    await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE_INDEX, '2019-01-02', jobId, '1'), data: { data: 'sss2' } });
                    await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE_INDEX, '2019-01-03', jobId, '2'), data: { data: 'sss3' } });

                    const rd = await adapter.listPrefixes({ path: BUCKETS_NAMES.HKUBE_INDEX });
                    expect(rd.includes('2019-01-01')).to.be.true;
                    expect(rd.includes('2019-01-02')).to.be.true;
                    expect(rd.includes('2019-01-03')).to.be.true;
                });
            });
            describe('put bucket key', () => {
                it('should throw error on invalid bucket name (empty)', (done) => {
                    adapter.createBucket({ Bucket: '  ' }).catch((error) => {
                        expect(error).to.be.an('error');
                        done();
                    });
                });
                it('should throw error on invalid bucket name (not string)', (done) => {
                    adapter.createBucket({ Bucket: 3424 }).catch((error) => {
                        expect(error).to.be.an('error');
                        done();
                    });
                });
                it('should throw error on invalid bucket name (null)', (done) => {
                    adapter.createBucket({ Bucket: null }).catch((error) => {
                        expect(error).to.be.an('error');
                        done();
                    });
                });
                it('should throw exception if bucket not exists', (done) => {
                    adapter.put({ Bucket: createJobId(), Key: createJobId(), Body: mock }).catch((error) => {
                        expect(error).to.be.an('error');
                        done();
                    });
                });
                it('put string as data', async () => {
                    const Bucket = 'yello';
                    const Key = 'yellow:yellow-algorithms:' + createJobId();
                    await adapter.createBucket({ Bucket });
                    await adapter.put({ Bucket, Key, Body: 'str' });
                    const result = await adapter.get({ Bucket, Key });
                    expect(result).to.equal('str');
                });
                it('get metadata', async () => {
                    const Bucket = 'yello';
                    const Key = 'yellow:yellow-algorithms:' + createJobId();
                    await adapter.createBucket({ Bucket });
                    const res = await adapter.put({ Bucket, Key, Body: 'str' });
                    const result = await adapter.getMetadata({ path: res.path });
                    expect(result).to.have.property('size')
                });
                it('put number as data', async () => {
                    const Bucket = 'green';
                    const Key = 'green:green-algorithms2:' + createJobId();
                    await adapter.createBucket({ Bucket });
                    await adapter.put({ Bucket, Key, Body: 123456 });
                    const result = await adapter.get({ Bucket, Key });
                    expect(result).to.equal(123456);
                });
                it('put object as data', async () => {
                    const Bucket = 'red';
                    const Key = 'red:red-algorithms2:' + createJobId();
                    await adapter.createBucket({ Bucket });
                    await adapter.put({ Bucket, Key, Body: mock });
                    const result = await adapter.get({ Bucket, Key });
                    expect(result).to.deep.equal(mock);
                });
                it('put array as data', async () => {
                    const Bucket = 'black';
                    const Key = 'black:black-algorithms2:' + createJobId();
                    await adapter.createBucket({ Bucket });
                    await adapter.put({ Bucket, Key, Body: [1, 2, 3] });
                    const result = await adapter.get({ Bucket, Key });
                    expect(result).to.include(1, 2, 3);

                    await adapter.put({ Bucket, Key, Body: [mock, mock] });
                    const result1 = await adapter.get({ Bucket, Key });
                    expect(result1).to.deep.include(mock, mock);
                });
                it('add multiple objects to the same bucket', async () => {
                    const Bucket = createJobId();
                    await adapter.createBucket({ Bucket });
                    await adapter.put({ Bucket, Key: createJobId(), Body: mock });
                    await adapter.put({ Bucket, Key: createJobId(), Body: mock });
                    await adapter.put({ Bucket, Key: createJobId(), Body: mock });
                    await adapter.put({ Bucket, Key: createJobId(), Body: mock });
                    await adapter.put({ Bucket, Key: createJobId(), Body: mock });
                });
                it('put-stream', async () => {
                    const Bucket = createJobId();
                    await adapter.createBucket({ Bucket });
                    const readStream = fs.createReadStream('tests/big-file.txt');
                    await adapter.put({ Bucket, Key: createJobId(), Body: readStream });
                    const readStream2 = fs.createReadStream('tests/big-file.txt');
                    await adapter.put({ Bucket, Key: createJobId(), Body: readStream2 });
                });
                it('put-buffer', async () => {
                    const Bucket = createJobId();
                    await adapter.createBucket({ Bucket });
                    const buf = Buffer.from('hello buffer');
                    await adapter.put({ Bucket, Key: createJobId(), Body: buf });
                });
                it('override', async () => {
                    const Bucket = createJobId();
                    const objectId = createJobId();
                    await adapter.createBucket({ Bucket });
                    const readStream = fs.createReadStream('tests/big-file.txt');
                    await adapter.put({ Bucket, Key: objectId, Body: readStream });
                    const readStream2 = fs.createReadStream('tests/big-file.txt');
                    await adapter.put({ Bucket, Key: objectId, Body: readStream2 });
                });
            });
            describe('get', () => {
                it('should get job result', async () => {
                    const Bucket = createJobId();
                    await adapter.createBucket({ Bucket });
                    await adapter.put({ Bucket, Key: 'result.json', Body: { data: 'test1' } });
                    expect(await adapter.get({ Bucket, Key: 'result.json' })).to.deep.equal({ data: 'test1' });
                });
                it('should get object by bucket & objectId', async () => {
                    const Bucket = createJobId();
                    const objectId1 = createJobId();
                    const objectId2 = createJobId();
                    const objectId3 = createJobId();
                    const objectId4 = createJobId();

                    await adapter.createBucket({ Bucket });
                    await adapter.put({ Bucket, Key: objectId1, Body: { data: 'test1' } });
                    await adapter.put({ Bucket, Key: objectId2, Body: { data: 'test2' } });
                    await adapter.put({ Bucket, Key: objectId3, Body: { data: 'test3' } });
                    await adapter.put({ Bucket, Key: objectId4, Body: { data: 'test4' } });

                    expect(await adapter.get({ Bucket, Key: objectId1 })).to.deep.equal({ data: 'test1' });
                    expect(await adapter.get({ Bucket, Key: objectId2 })).to.deep.equal({ data: 'test2' });
                    expect(await adapter.get({ Bucket, Key: objectId3 })).to.deep.equal({ data: 'test3' });
                    expect(await adapter.get({ Bucket, Key: objectId4 })).to.deep.equal({ data: 'test4' });
                });
                it('should failed if bucket not exists', (done) => {
                    adapter.get({ Bucket: createJobId(), Key: createJobId() }).catch((err) => {
                        expect(err.statusCode).to.equal(404);
                        expect(err.name).to.equal('NoSuchBucket');
                        done();
                    });
                });
                it('should failed if objectId not exists', (done) => {
                    const Bucket = createJobId();
                    const resolvingPromise = new Promise((resolve, reject) => {
                        adapter.createBucket({ Bucket }).then(() => {
                            adapter.put({ Bucket, Key: createJobId(), Body: mock }).then(() => {
                                adapter.get({ Bucket, Key: createJobId() }).catch(err => reject(err));
                            });
                        });
                    });

                    resolvingPromise.catch((err) => {
                        expect(err.statusCode).to.equal(404);
                        expect(err.name).to.equal('NoSuchKey');
                        done();
                    });
                });
            });
            describe('getStream', () => {
                it('get-stream file', async () => {
                    const Bucket = createJobId();
                    const Key = createJobId();
                    await adapter.createBucket({ Bucket });

                    const readStream = fs.createReadStream('tests/big-file.txt');
                    const resT = await adapter.put({ Bucket, Key, Body: readStream });
                    const res = await adapter.getStream({ path: resT.path });
                    await new Promise((resolve, reject) => {
                        res.pipe(fs.createWriteStream('tests/dest.txt'))
                            .on('error', (err) => {
                                reject(err);
                            }).on('finish', () => {
                                resolve();
                            });
                    });
                });
                it('get-stream', async () => {
                    const Bucket = createJobId();
                    const Key = createJobId();
                    await adapter.createBucket({ Bucket });

                    const streamObject = new stream.Readable();
                    const array = Buffer.alloc(1000, 'dd');

                    streamObject.push(array);
                    streamObject.push(null);

                    const result = await adapter.put({ Bucket, Key, Body: streamObject, ignoreEncode: true });
                    const streamRes = await adapter.getStream({ ...result, start: 0, end: 5, ignoreEncode: true });
                    await new Promise((resolve) => {
                        const bufs = [];
                        streamRes.on('data', (d) => { bufs.push(d); });
                        streamRes.on('end', () => {
                            const buf = Buffer.concat(bufs);
                            expect(buf).to.deep.equal(Buffer.alloc(6, 'dd'));
                            resolve();
                        });
                    });
                });
                it('get-buffer', async () => {
                    const Bucket = createJobId();
                    const Key = createJobId();
                    await adapter.createBucket({ Bucket });
                    const buf = Buffer.from('hello buffer');
                    await adapter.put({ Bucket, Key, Body: buf });
                    await adapter.getBuffer({ Bucket, Key });
                });
            });
            describe('seek', () => {
                it('seek no start end', async () => {
                    const Bucket = createJobId();
                    const Key = createJobId();
                    await adapter.createBucket({ Bucket });
                    const buffer = Buffer.from([1, 2, 3, 4, 5, 6, 7, 8, 9]);
                    const result = await adapter.put({ Bucket, Key, Body: buffer, ignoreEncode: true });
                    const res = await adapter.seek({ path: result.path, ignoreEncode: true });
                    expect(res).to.deep.equal(buffer);
                });
                it('seek start: 2', async () => {
                    const Bucket = createJobId();
                    const Key = createJobId();
                    await adapter.createBucket({ Bucket });
                    const buffer = Buffer.from([1, 2, 3, 4, 5, 6, 7, 8, 9]);
                    const result = await adapter.put({ Bucket, Key, Body: buffer, ignoreEncode: true });
                    const res = await adapter.seek({ path: result.path, start: 2, ignoreEncode: true })
                    expect(res).to.deep.equal(Buffer.from([3, 4, 5, 6, 7, 8, 9]));
                });
                it('seek start: 2, end: 0', async () => {
                    const Bucket = createJobId();
                    const Key = createJobId();
                    await adapter.createBucket({ Bucket });
                    const buffer = Buffer.from([1, 2, 3, 4, 5, 6, 7, 8, 9]);
                    const result = await adapter.put({ Bucket, Key, Body: buffer, ignoreEncode: true });
                    expect(adapter.seek({ path: result.path, start: 2, end: 0, ignoreEncode: true })).to.eventually.rejectedWith('invalid range');
                });
                it('seek start: 0 end: -2', async () => {
                    const Bucket = createJobId();
                    const Key = createJobId();
                    await adapter.createBucket({ Bucket });
                    const buffer = Buffer.from([1, 2, 3, 4, 5, 6, 7, 8, 9]);
                    const result = await adapter.put({ Bucket, Key, Body: buffer, ignoreEncode: true });
                    const res = await adapter.seek({ path: result.path, start: 0, end: -2, ignoreEncode: true });
                    expect(res).to.deep.equal(Buffer.from([1, 2, 3, 4, 5, 6, 7, 8]));
                });
                it('seek start: 2 end: -2', async () => {
                    const Bucket = createJobId();
                    const Key = createJobId();
                    await adapter.createBucket({ Bucket });
                    const buffer = Buffer.from([1, 2, 3, 4, 5, 6, 7, 8, 9]);
                    const result = await adapter.put({ Bucket, Key, Body: buffer, ignoreEncode: true });
                    const res = await adapter.seek({ path: result.path, start: 2, end: -2, ignoreEncode: true });
                    expect(res).to.deep.equal(Buffer.from([3, 4, 5, 6, 7, 8]));
                });
                it('seek start: 0 end: 6', async () => {
                    const Bucket = createJobId();
                    const Key = createJobId();
                    await adapter.createBucket({ Bucket });
                    const buffer = Buffer.from([1, 2, 3, 4, 5, 6, 7, 8, 9]);
                    const result = await adapter.put({ Bucket, Key, Body: buffer, ignoreEncode: true });
                    const res = await adapter.seek({ path: result.path, start: 0, end: 6, ignoreEncode: true });
                    expect(res).to.deep.equal(Buffer.from([1, 2, 3, 4, 5, 6, 7]));
                });
                it('seek end: -6', async () => {
                    const Bucket = createJobId();
                    const Key = createJobId();
                    await adapter.createBucket({ Bucket });
                    const buffer = Buffer.from([1, 2, 3, 4, 5, 6, 7, 8, 9]);
                    const result = await adapter.put({ Bucket, Key, Body: buffer, ignoreEncode: true });
                    const res = await adapter.seek({ path: result.path, end: -6, ignoreEncode: true });
                    expect(res).to.deep.equal(Buffer.from([4, 5, 6, 7, 8, 9]));
                });
            });
            describe('bucket name validations', () => {
                it('Bucket name is longer than 63 characters', (done) => {
                    const Bucket = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx:' + createJobId();
                    adapter.createBucket({ Bucket }).catch((error) => {
                        expect(error).to.be.an('error');
                        done();
                    });
                });
                it('create bucket with LocationConstraint', async () => {
                    const Bucket = 'sss' + createJobId();
                    await adapter.createBucket({
                        Bucket,
                        CreateBucketConfiguration: { LocationConstraint: 'eu-west-1' }
                    });
                    expect(await adapter._isBucketExists({ Bucket })).to.equal(true);
                });
                it('create bucket with empty string LocationConstraint', async () => {
                    const Bucket = 'sss' + createJobId();
                    await adapter.createBucket({
                        Bucket,
                        CreateBucketConfiguration: { LocationConstraint: '' }
                    });
                    expect(await adapter._isBucketExists({ Bucket })).to.equal(true);
                });
            });
            describe('listObjects', () => {
                it('get 10 keys', async () => {
                    const Bucket = 'test-list-keys';
                    await adapter.createBucket({ Bucket });
                    const promiseArray = [];
                    for (let i = 0; i < 10; i += 1) {
                        promiseArray.push(adapter.put({ Bucket, Key: `test1/test-${i}/xxx.json`, Body: { value: 'str' } }));
                    }
                    await Promise.all(promiseArray);
                    const objects = await adapter.listObjects({ Bucket, Prefix: 'test1' });
                    expect(objects.length).to.equal(10);
                });
                it('get 10 objects', async () => {
                    const Bucket = 'test-list-objects';
                    await adapter.createBucket({ Bucket });
                    const promiseArray = [];
                    for (let i = 0; i < 10; i += 1) {
                        promiseArray.push(adapter.put({ Bucket, Key: `test1/test/xxx-${i}.json`, Body: { value: 'str' } }));
                    }
                    await Promise.all(promiseArray);
                    const objects = await adapter.listObjects({ Bucket, Prefix: 'test1/test' });
                    expect(objects.length).to.equal(10);
                });
                it('get more than 1000 keys', async () => {
                    const Bucket = 'test-1000-keys';
                    await adapter.createBucket({ Bucket });
                    const promiseArray = [];
                    for (let i = 0; i < 1500; i += 1) {
                        promiseArray.push(adapter.put({ Bucket, Key: `test1/test-${i}/xxx.json`, Body: { value: 'str' } }));
                    }
                    await Promise.all(promiseArray);
                    const objects = await adapter.listObjects({ Bucket, Prefix: 'test1' });
                    expect(objects.length).to.equal(1500);
                });
                it('get more than 1000 objects', async () => {
                    const Bucket = 'test-1000-objects';
                    await adapter.createBucket({ Bucket });
                    const promiseArray = [];
                    for (let i = 0; i < 1500; i += 1) {
                        promiseArray.push(adapter.put({ Bucket, Key: `test1/test/xxx-${i}.json`, Body: { value: 'str' } }));
                    }
                    await Promise.all(promiseArray);
                    const objects = await adapter.listObjects({ Bucket, Prefix: 'test1/test' });
                    expect(objects.length).to.equal(1500);
                });
                it('list by delimiter', async () => {
                    const Bucket = uniqid();
                    await adapter.createBucket({ Bucket });

                    await adapter.put({ Bucket, Key: '2019-01-27/test1', Body: { data: 'test3' } });
                    await adapter.put({ Bucket, Key: '2019-01-26/test2', Body: { data: 'test3' } });
                    await adapter.put({ Bucket, Key: '2019-01-25/test1', Body: { data: 'test3' } });

                    const prefixes = await adapter.listByDelimiter({
                        Bucket,
                        Delimiter: '/'
                    });
                    expect(prefixes.length).to.equal(3);
                });
            });
            describe('delete', () => {
                it('get 10 objects', async () => {
                    const Bucket = 'delete-objects';
                    await adapter.createBucket({ Bucket });
                    const promiseArray = [];
                    for (let i = 0; i < 10; i += 1) {
                        promiseArray.push(adapter.put({ Bucket, Key: `test1/test/xxx-${i}.json`, Body: { value: 'str' } }));
                    }
                    await Promise.all(promiseArray);
                    const objectsToDelete = await adapter.listObjects({ Bucket, Prefix: 'test1/test' });
                    await adapter.deleteObjects({ Bucket, Delete: { Objects: objectsToDelete } });
                    const objectsToDeleteAfter = await adapter.listObjects({ Bucket, Prefix: 'test1/test' });
                    expect(objectsToDeleteAfter.length).to.equal(0);
                });
                it('get 10 keys', async () => {
                    const Bucket = 'delete-keys';
                    await adapter.createBucket({ Bucket });
                    const promiseArray = [];
                    for (let i = 0; i < 10; i += 1) {
                        promiseArray.push(adapter.put({ Bucket, Key: `test1/test-${i}/xxx.json`, Body: { value: 'str' } }));
                    }
                    await Promise.all(promiseArray);

                    const objectsToDelete = await adapter.listObjects({ Bucket, Prefix: 'test1' });
                    await adapter.deleteObjects({ Bucket, Delete: { Objects: objectsToDelete } });
                    const objectsToDeleteAfter = await adapter.listObjects({ Bucket, Prefix: 'test1' });
                    expect(objectsToDeleteAfter.length).to.equal(0);
                });
            });
        });
    });
});