const { expect } = require('chai');
const path = require('path');
const adapter = require('../lib/s3-adapter');
const moment = require('moment');
const BUCKETS_NAMES = {
    HKUBE: 'hkube',
    HKUBE_RESULTS: 'hkube-results',
    HKUBE_METADATA: 'hkube-metadata',
    HKUBE_STORE: 'hkube-store',
    HKUBE_EXECUTION: 'hkube-execution'
};
const DateFormat = 'YYYY-MM-DD';

describe('s3-adapter', () => {
    before(async () => {
        const options = {
            accessKeyId: process.env.AWS_ACCESS_KEY_ID || 'AKIAIOSFODNN7EXAMPLE',
            secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
            endpoint: process.env.AWS_ENDPOINT || 'http://127.0.0.1:9000'
        };
        await adapter.init(options, null, BUCKETS_NAMES, true);
    });
    describe('put', () => {
        it('put and get same value', async () => {
            await adapter.put({ Path: path.join(BUCKETS_NAMES.HKUBE, moment('2015-01-27').format(DateFormat), 'test1', 'test1.json'), Data: { data: 'sss' } });
            await adapter.put({ Path: path.join(BUCKETS_NAMES.HKUBE, moment('2015-01-26').format(DateFormat), 'test1', 'test1.json'), Data: { data: 'sss' } });
            await adapter.put({ Path: path.join(BUCKETS_NAMES.HKUBE, moment('2015-01-25').format(DateFormat), 'test1', 'test1.json'), Data: { data: 'sss' } });
            await adapter.put({ Path: path.join(BUCKETS_NAMES.HKUBE, moment('2015-01-24').format(DateFormat), 'test1', 'test1.json'), Data: { data: 'sss' } });
            await adapter.put({ Path: path.join(BUCKETS_NAMES.HKUBE, moment('2015-01-23').format(DateFormat), 'test1', 'test1.json'), Data: { data: 'sss' } });
            await adapter.put({ Path: path.join(BUCKETS_NAMES.HKUBE, moment('2015-01-22').format(DateFormat), 'test1', 'test1.json'), Data: { data: 'sss' } });
            await adapter.put({ Path: path.join(BUCKETS_NAMES.HKUBE, moment('2015-01-21').format(DateFormat), 'test1', 'test1.json'), Data: { data: 'sss' } });

            await adapter.put({ Path: path.join(BUCKETS_NAMES.HKUBE, moment('2018-06-21').format(DateFormat), 'test1', 'test1.json'), Data: { data: 'sss' } });
            await adapter.put({ Path: path.join(BUCKETS_NAMES.HKUBE, moment('2018-06-22').format(DateFormat), 'test1', 'test1.json'), Data: { data: 'sss' } });
            await adapter.put({ Path: path.join(BUCKETS_NAMES.HKUBE, moment('2018-06-23').format(DateFormat), 'test1', 'test1.json'), Data: { data: 'sss' } });
            await adapter.put({ Path: path.join(BUCKETS_NAMES.HKUBE, moment('2018-06-24').format(DateFormat), 'test1', 'test1.json'), Data: { data: 'sss' } });
            await adapter.put({ Path: path.join(BUCKETS_NAMES.HKUBE, moment('2018-06-25').format(DateFormat), 'test1', 'test1.json'), Data: { data: 'sss' } });

            const link = await adapter.put({ Path: path.join(BUCKETS_NAMES.HKUBE, moment().format(DateFormat), Date.now().toString(), 'task-1'), Data: 'test' });
            const res = await adapter.get(link);
            expect(res).to.equal('test');
        });
        it('put result', async () => {
            const link = await adapter.put({ Path: path.join(BUCKETS_NAMES.HKUBE_RESULTS, moment().format(DateFormat), 'job-id', 'result.json'), Data: 'test' });
            const res = await adapter.get(link);
            expect(res).to.equal('test');
        });
        it('get all tasks of specific jobid', async () => {
            const jobId = Date.now();
            const results = await Promise.all([
                adapter.put({ Path: path.join(BUCKETS_NAMES.HKUBE, `${moment().format(DateFormat)}/${jobId}/0`), Data: 'test0' }),
                adapter.put({ Path: path.join(BUCKETS_NAMES.HKUBE, `${moment().format(DateFormat)}/${jobId}/1`), Data: 'test1' }),
                adapter.put({ Path: path.join(BUCKETS_NAMES.HKUBE, `${moment().format(DateFormat)}/${jobId}/2`), Data: 'test2' }),
                adapter.put({ Path: path.join(BUCKETS_NAMES.HKUBE, `${moment().format(DateFormat)}/${jobId}/3`), Data: 'test3' }),
                adapter.put({ Path: path.join(BUCKETS_NAMES.HKUBE, `${moment().format(DateFormat)}/${jobId}/4`), Data: 'test4' }),
                adapter.put({ Path: path.join(BUCKETS_NAMES.HKUBE, `${moment().format(DateFormat)}/${jobId}/5`), Data: 'test5' }),
                adapter.put({ Path: path.join(BUCKETS_NAMES.HKUBE, `${moment().format(DateFormat)}/${jobId}/6`), Data: 'test6' })]);

            const res = await adapter.list({ Path: path.join(BUCKETS_NAMES.HKUBE, moment().format(DateFormat), jobId.toString()) });
            expect(res.length).to.equal(7);

            for (let i = 0; i < results.length; i += 1) {
                const r = await adapter.get(results[i]);
                expect(r).to.equal('test' + i);
            }
        });
        it('get more than 3000 items', async () => {
            const promiseArray = [];
            for (let i = 0; i < 3500; i += 1) {
                promiseArray.push(adapter.put({ Path: path.join(BUCKETS_NAMES.HKUBE, moment().format(DateFormat), 'more-than-3000-keys', 'task' + i), Data: `test${i}` }));
            }
            await Promise.all(promiseArray);
            const res = await adapter.list({ Path: path.join(BUCKETS_NAMES.HKUBE, moment().format(DateFormat), 'more-than-3000-keys') });
            expect(res.length).to.equal(3500);
        }).timeout(40000);
        it('delete by date', async () => {
            await adapter.put({ Path: path.join(BUCKETS_NAMES.HKUBE, moment('2015-01-14').format(DateFormat), 'test1', 'test1.json'), Data: { data: 'sss' } });
            await adapter.put({ Path: path.join(BUCKETS_NAMES.HKUBE, moment('2015-01-14').format(DateFormat), 'test2', 'test2.json'), Data: { data: 'sss' } });
            await adapter.put({ Path: path.join(BUCKETS_NAMES.HKUBE, moment('2015-01-14').format(DateFormat), 'test3', 'test3.json'), Data: { data: 'sss' } });
            await adapter.put({ Path: path.join(BUCKETS_NAMES.HKUBE, moment('2015-01-14').format(DateFormat), 'test4', 'test4.json'), Data: { data: 'sss' } });
            await adapter.put({ Path: path.join(BUCKETS_NAMES.HKUBE, moment('2015-01-14').format(DateFormat), 'test1', 'test2.json'), Data: { data: 'sss' } });
            await adapter.put({ Path: path.join(BUCKETS_NAMES.HKUBE, moment('2015-01-14').format(DateFormat), 'test2', 'test3.json'), Data: { data: 'sss' } });
            await adapter.put({ Path: path.join(BUCKETS_NAMES.HKUBE, moment('2015-01-14').format(DateFormat), 'test3', 'test4.json'), Data: { data: 'sss' } });
            await adapter.put({ Path: path.join(BUCKETS_NAMES.HKUBE, moment('2015-01-14').format(DateFormat), 'test3', 'test5.json'), Data: { data: 'sss' } });
            await adapter.put({ Path: path.join(BUCKETS_NAMES.HKUBE, moment('2015-01-14').format(DateFormat), 'test3', 'test6.json'), Data: { data: 'sss' } });
            await adapter.put({ Path: path.join(BUCKETS_NAMES.HKUBE, moment('2015-01-14').format(DateFormat), 'test3', 'test7.json'), Data: { data: 'sss' } });

            const res = await adapter.delete({ Path: path.join(BUCKETS_NAMES.HKUBE, '2015-01-14') });
            expect(res.Deleted.length).to.equal(10);

            await adapter.put({ Path: path.join(BUCKETS_NAMES.HKUBE, moment('2015-01-14').format(DateFormat), 'test1', 'test1.json'), Data: { data: 'sss' } });
            await adapter.put({ Path: path.join(BUCKETS_NAMES.HKUBE, moment('2015-01-14').format(DateFormat), 'test2', 'test2.json'), Data: { data: 'sss' } });
            await adapter.put({ Path: path.join(BUCKETS_NAMES.HKUBE, moment('2015-01-14').format(DateFormat), 'test3', 'test3.json'), Data: { data: 'sss' } });
            await adapter.put({ Path: path.join(BUCKETS_NAMES.HKUBE, moment('2015-01-14').format(DateFormat), 'test4', 'test4.json'), Data: { data: 'sss' } });
            await adapter.put({ Path: path.join(BUCKETS_NAMES.HKUBE, moment('2015-01-14').format(DateFormat), 'test1', 'test2.json'), Data: { data: 'sss' } });
            await adapter.put({ Path: path.join(BUCKETS_NAMES.HKUBE, moment('2015-01-14').format(DateFormat), 'test2', 'test3.json'), Data: { data: 'sss' } });
            await adapter.put({ Path: path.join(BUCKETS_NAMES.HKUBE, moment('2015-01-14').format(DateFormat), 'test3', 'test4.json'), Data: { data: 'sss' } });
            await adapter.put({ Path: path.join(BUCKETS_NAMES.HKUBE, moment('2015-01-14').format(DateFormat), 'test3', 'test5.json'), Data: { data: 'sss' } });
            await adapter.put({ Path: path.join(BUCKETS_NAMES.HKUBE, moment('2015-01-14').format(DateFormat), 'test3', 'test6.json'), Data: { data: 'sss' } });
            await adapter.put({ Path: path.join(BUCKETS_NAMES.HKUBE, moment('2015-01-14').format(DateFormat), 'test3', 'test7.json'), Data: { data: 'sss' } });

            const res1 = await adapter.delete({ Path: path.join(BUCKETS_NAMES.HKUBE, '2015-01-14/test1') });
            expect(res1.Deleted.length).to.equal(2);

            const res2 = await adapter.delete({ Path: path.join(BUCKETS_NAMES.HKUBE, '2015-01-14/test2') });
            expect(res2.Deleted.length).to.equal(2);

            const res3 = await adapter.delete({ Path: path.join(BUCKETS_NAMES.HKUBE, '2015-01-14/test3') });
            expect(res3.Deleted.length).to.equal(5);
        }).timeout(5000);
        it('delete by date more than 3000 items', async () => {
            const promiseArray = [];
            for (let i = 0; i < 3500; i += 1) {
                promiseArray.push(adapter.put({ Path: path.join(BUCKETS_NAMES.HKUBE, moment('2014-11-28').format(DateFormat), 'test3', `test${i}.json`), Data: { data: 'sss' } }));
            }
            await Promise.all(promiseArray);
            const res = await adapter.delete({ Path: path.join(BUCKETS_NAMES.HKUBE, '2014-11-28') });
            expect(res.Deleted.length).to.equal(3500);
        }).timeout(40000);
        it('list objects without prefix', async () => {
            const jobId = Date.now().toString();
            await Promise.all([
                adapter.put({ Path: path.join(BUCKETS_NAMES.HKUBE, moment().format(DateFormat), jobId, '0'), Data: 'test0' }),
                adapter.put({ Path: path.join(BUCKETS_NAMES.HKUBE, moment().format(DateFormat), jobId, '0'), Data: 'test0' }),
                adapter.put({ Path: path.join(BUCKETS_NAMES.HKUBE_RESULTS, moment().format(this.DateFormat), jobId, 'result.json'), Data: 'test0' }),
                adapter.put({ Path: path.join(BUCKETS_NAMES.HKUBE_RESULTS, moment().format(this.DateFormat), jobId, 'result.json'), Data: 'test6' })]);
            const res1 = await adapter.list({ Path: path.join(BUCKETS_NAMES.HKUBE, '/') });
            expect(res1.length > 0).to.be.true;
            const res2 = await adapter.list({ Path: path.join(BUCKETS_NAMES.HKUBE_RESULTS, '/') });
            expect(res2.length > 0).to.be.true;
        }).timeout(40000);
    });
});
