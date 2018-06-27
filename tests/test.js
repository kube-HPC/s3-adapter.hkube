const { expect } = require('chai');
const adapter = require('../lib/s3-adapter');
const moment = require('moment');
const DATE_FORMAT = 'DD-MM-YYYY';
const { BUCKETS_NAMES } = require('../consts/buckets-names');

describe('s3-adapter', () => {
    before((done) => {
        const options = {
            accessKeyId: process.env.AWS_ACCESS_KEY_ID || 'AKIAIOSFODNN7EXAMPLE',
            secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
            endpoint: process.env.AWS_ENDPOINT || 'http://127.0.0.1:9000'
        };
        adapter.init(options, null, true);
        done();
    });
    describe('put', () => {
        it('put and get same value', async () => {
            const link = await adapter.put({ jobId: Date.now(), taskId: 'task-1', data: 'test' });
            const res = await adapter.get(link);
            expect(res).to.equal('test');
        });
        it('put result', async () => {
            const link = await adapter.putResults({ jobId: 'same-value-test', data: 'test' });
            const res = await adapter.get(link);
            expect(res).to.equal('test');
        });
        it('get all tasks of specific jobid', async () => {
            const jobId = Date.now();
            const results = await Promise.all([
                adapter.put({ jobId, taskId: '0', data: 'test0' }),
                adapter.put({ jobId, taskId: '1', data: 'test1' }),
                adapter.put({ jobId, taskId: '2', data: 'test2' }),
                adapter.put({ jobId, taskId: '3', data: 'test3' }),
                adapter.put({ jobId, taskId: '4', data: 'test4' }),
                adapter.put({ jobId, taskId: '5', data: 'test5' }),
                adapter.put({ jobId, taskId: '6', data: 'test6' })]);

            const res = await adapter.listObjects({ Filter: `${moment().format(DATE_FORMAT)}/${jobId}` });
            expect(res.length).to.equal(7);

            for (let i = 0; i < results.length; i += 1) {
                const r = await adapter.get(results[i]);
                expect(r).to.equal('test' + i);
            }
        });
        it('get more than 3000 items', async () => {
            const promiseArray = [];
            for (let i = 0; i < 3500; i += 1) {
                promiseArray.push(adapter.put({ jobId: 'more-than-3000-keys', taskId: 'task' + i, data: 'test' + i }));
            }
            await Promise.all(promiseArray);
            const res = await adapter.listObjects({ Filter: `${moment().format(DATE_FORMAT)}/more-than-3000-keys` });
            expect(res.length).to.equal(3500);
        }).timeout(40000);
        it('delete by date', async () => {
            await adapter._put({ Bucket: BUCKETS_NAMES.HKUBE, Key: `${moment('2015-01-10').format(DATE_FORMAT)}/test1/test1.json`, Body: { data: 'sss' } });
            await adapter._put({ Bucket: BUCKETS_NAMES.HKUBE, Key: `${moment('2015-01-10').format(DATE_FORMAT)}/test2/test2.json`, Body: { data: 'sss' } });
            await adapter._put({ Bucket: BUCKETS_NAMES.HKUBE, Key: `${moment('2015-01-10').format(DATE_FORMAT)}/test3/test3.json`, Body: { data: 'sss' } });
            await adapter._put({ Bucket: BUCKETS_NAMES.HKUBE, Key: `${moment('2015-01-10').format(DATE_FORMAT)}/test4/test4.json`, Body: { data: 'sss' } });
            await adapter._put({ Bucket: BUCKETS_NAMES.HKUBE, Key: `${moment('2015-01-10').format(DATE_FORMAT)}/test1/test2.json`, Body: { data: 'sss' } });
            await adapter._put({ Bucket: BUCKETS_NAMES.HKUBE, Key: `${moment('2015-01-10').format(DATE_FORMAT)}/test2/test3.json`, Body: { data: 'sss' } });
            await adapter._put({ Bucket: BUCKETS_NAMES.HKUBE, Key: `${moment('2015-01-10').format(DATE_FORMAT)}/test3/test4.json`, Body: { data: 'sss' } });
            await adapter._put({ Bucket: BUCKETS_NAMES.HKUBE, Key: `${moment('2015-01-10').format(DATE_FORMAT)}/test3/test5.json`, Body: { data: 'sss' } });
            await adapter._put({ Bucket: BUCKETS_NAMES.HKUBE, Key: `${moment('2015-01-10').format(DATE_FORMAT)}/test3/test6.json`, Body: { data: 'sss' } });
            await adapter._put({ Bucket: BUCKETS_NAMES.HKUBE, Key: `${moment('2015-01-10').format(DATE_FORMAT)}/test3/test7.json`, Body: { data: 'sss' } });

            const res = await adapter.deleteByDate({ Date: new Date('2015-01-10') });
            expect(res.Deleted.length).to.equal(11);
        }).timeout(5000);
        it('delete by date more than 3000 items', async () => {
            const promiseArray = [];
            for (let i = 0; i < 3500; i += 1) {
                promiseArray.push(adapter._put({ Bucket: BUCKETS_NAMES.HKUBE, Key: `${moment('2014-11-11').format(DATE_FORMAT)}/test3/test${i}.json`, Body: { data: 'sss' } }));
            }
            await Promise.all(promiseArray);

            const res = await adapter.deleteByDate({ Date: new Date('2014-11-11') });
            expect(res.Deleted.length).to.equal(3501);
        }).timeout(40000);
    });
    describe('jobPath', () => {
        it('jobPath', async () => {
            await adapter.jobPath({ jobId: 'same-value-test', taskId: 'task-1', data: 'test' });
        });
    });
});
