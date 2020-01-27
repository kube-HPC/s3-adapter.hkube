const { expect } = require('chai');
const path = require('path');
const S3Adapter = require('../lib/s3-adapter');
const moment = require('moment');
const BUCKETS_NAMES = {
    HKUBE: 'hkube',
    HKUBE_RESULTS: 'hkube-results',
    HKUBE_METADATA: 'hkube-metadata',
    HKUBE_STORE: 'hkube-store',
    HKUBE_EXECUTION: 'hkube-execution',
    HKUBE_INDEX: 'hkube-index'
};
const DateFormat = 'YYYY-MM-DD';
const extraOptions = [{ binary: false }, { binary: true }];
const adapter = new S3Adapter();
extraOptions.forEach((o) => {
    describe(`s3-adapter ${o.binary ? 'binary' : 'json'}`, () => {
        before(async () => {
            const options = {
                accessKeyId: process.env.AWS_ACCESS_KEY_ID || 'AKIAIOSFODNN7EXAMPLE',
                secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
                endpoint: process.env.AWS_ENDPOINT || 'http://127.0.0.1:9000',
                ...o
            };
            adapter._wasInit = false;
            await adapter.init(options, BUCKETS_NAMES, true);
        });
        describe(`put ${o.binary ? 'binary' : 'json'}`, () => {
            it(`put and get same value ${o.binary ? 'binary' : 'json'}`, async () => {
                await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, `${o.binary ? 'binary' : 'json'}`, moment('2015-01-27').format(DateFormat), 'test1', 'test1.json'), data: { data: 'sss' } });
                await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, `${o.binary ? 'binary' : 'json'}`, moment('2015-01-26').format(DateFormat), 'test1', 'test1.json'), data: { data: 'sss' } });
                await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, `${o.binary ? 'binary' : 'json'}`, moment('2015-01-25').format(DateFormat), 'test1', 'test1.json'), data: { data: 'sss' } });
                await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, `${o.binary ? 'binary' : 'json'}`, moment('2015-01-24').format(DateFormat), 'test1', 'test1.json'), data: { data: 'sss' } });
                await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, `${o.binary ? 'binary' : 'json'}`, moment('2015-01-23').format(DateFormat), 'test1', 'test1.json'), data: { data: 'sss' } });
                await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, `${o.binary ? 'binary' : 'json'}`, moment('2015-01-22').format(DateFormat), 'test1', 'test1.json'), data: { data: 'sss' } });
                await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, moment('2015-01-21').format(DateFormat), 'test1', 'test1.json'), data: { data: 'sss' } });

                await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, moment('2018-06-21').format(DateFormat), 'test1', 'test1.json'), data: { data: 'sss' } });
                await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, moment('2018-06-22').format(DateFormat), 'test1', 'test1.json'), data: { data: 'sss' } });
                await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, moment('2018-06-23').format(DateFormat), 'test1', 'test1.json'), data: { data: 'sss' } });
                await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, moment('2018-06-24').format(DateFormat), 'test1', 'test1.json'), data: { data: 'sss' } });
                await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, moment('2018-06-25').format(DateFormat), 'test1', 'test1.json'), data: { data: 'sss' } });

                const link = await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, moment().format(DateFormat), Date.now().toString(), 'task-1'), data: 'test' });
                const res = await adapter.get(link);
                expect(res).to.equal('test');
            });
            it(`put result ${o.binary ? 'binary' : 'json'}`, async () => {
                const link = await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE_RESULTS, `${o.binary ? 'binary' : 'json'}`, moment().format(DateFormat), 'job-id', 'result.json'), data: 'test' });
                const res = await adapter.get(link);
                expect(res).to.equal('test');
            });
            it(`get all tasks of specific jobid ${o.binary ? 'binary' : 'json'}`, async () => {
                const jobId = Date.now();
                const results = await Promise.all([
                    adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, `${moment().format(DateFormat)}/${jobId}/0`), data: 'test0' }),
                    adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, `${moment().format(DateFormat)}/${jobId}/1`), data: 'test1' }),
                    adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, `${moment().format(DateFormat)}/${jobId}/2`), data: 'test2' }),
                    adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, `${moment().format(DateFormat)}/${jobId}/3`), data: 'test3' }),
                    adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, `${moment().format(DateFormat)}/${jobId}/4`), data: 'test4' }),
                    adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, `${moment().format(DateFormat)}/${jobId}/5`), data: 'test5' }),
                    adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, `${moment().format(DateFormat)}/${jobId}/6`), data: 'test6' })]);

                const res = await adapter.list({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, moment().format(DateFormat), jobId.toString()) });
                expect(res.length).to.equal(7);

                for (let i = 0; i < results.length; i += 1) {
                    const r = await adapter.get(results[i]);
                    expect(r).to.equal('test' + i);
                }
            });
            it(`get more than 3000 items ${o.binary ? 'binary' : 'json'}`, async () => {
                const promiseArray = [];
                for (let i = 0; i < 3500; i += 1) {
                    promiseArray.push(adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, `${o.binary ? 'binary' : 'json'}`, moment().format(DateFormat), 'more-than-3000-keys', 'task' + i), data: `test${i}` }));
                }
                await Promise.all(promiseArray);
                const res = await adapter.list({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, `${o.binary ? 'binary' : 'json'}`, moment().format(DateFormat), 'more-than-3000-keys') });
                expect(res.length).to.equal(3500);
            }).timeout(80000);
            it(`delete more than 3000 items ${o.binary ? 'binary' : 'json'}`, async () => {
                {
                    const promiseArray = [];
                    for (let i = 0; i < 3500; i += 1) {
                        promiseArray.push(adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, moment().format(DateFormat), 'more-than-3000-keys2', 'task' + i), data: `test${i}` }));
                    }
                    await Promise.all(promiseArray);
                    const res = await adapter.list({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, moment().format(DateFormat), 'more-than-3000-keys2') });
                    expect(res.length).to.equal(3500);

                    await adapter.delete({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, moment().format(DateFormat), 'more-than-3000-keys2') });
                    const res2 = await adapter.list({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, moment().format(DateFormat), 'more-than-3000-keys2') });
                    expect(res2.length).to.equal(0);
                }
            }).timeout(80000);
            it(`delete by date ${o.binary ? 'binary' : 'json'}`, async () => {
                await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, moment('2015-01-14').format(DateFormat), 'test1', 'test1.json'), data: { data: 'sss' } });
                await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, moment('2015-01-14').format(DateFormat), 'test2', 'test2.json'), data: { data: 'sss' } });
                await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, moment('2015-01-14').format(DateFormat), 'test3', 'test3.json'), data: { data: 'sss' } });
                await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, moment('2015-01-14').format(DateFormat), 'test4', 'test4.json'), data: { data: 'sss' } });
                await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, moment('2015-01-14').format(DateFormat), 'test1', 'test2.json'), data: { data: 'sss' } });
                await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, moment('2015-01-14').format(DateFormat), 'test2', 'test3.json'), data: { data: 'sss' } });
                await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, moment('2015-01-14').format(DateFormat), 'test3', 'test4.json'), data: { data: 'sss' } });
                await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, moment('2015-01-14').format(DateFormat), 'test3', 'test5.json'), data: { data: 'sss' } });
                await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, moment('2015-01-14').format(DateFormat), 'test3', 'test6.json'), data: { data: 'sss' } });
                await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, moment('2015-01-14').format(DateFormat), 'test3', 'test7.json'), data: { data: 'sss' } });

                await adapter.delete({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, '2015-01-14') });
                const res0 = await adapter.list({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, '2015-01-14') });
                expect(res0.length).to.equal(0);

                await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, moment('2015-01-14').format(DateFormat), 'test1', 'test1.json'), data: { data: 'sss' } });
                await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, moment('2015-01-14').format(DateFormat), 'test2', 'test2.json'), data: { data: 'sss' } });
                await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, moment('2015-01-14').format(DateFormat), 'test3', 'test3.json'), data: { data: 'sss' } });
                await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, moment('2015-01-14').format(DateFormat), 'test4', 'test4.json'), data: { data: 'sss' } });
                await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, moment('2015-01-14').format(DateFormat), 'test1', 'test2.json'), data: { data: 'sss' } });
                await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, moment('2015-01-14').format(DateFormat), 'test2', 'test3.json'), data: { data: 'sss' } });
                await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, moment('2015-01-14').format(DateFormat), 'test3', 'test4.json'), data: { data: 'sss' } });
                await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, moment('2015-01-14').format(DateFormat), 'test3', 'test5.json'), data: { data: 'sss' } });
                await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, moment('2015-01-14').format(DateFormat), 'test3', 'test6.json'), data: { data: 'sss' } });
                await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, moment('2015-01-14').format(DateFormat), 'test3', 'test7.json'), data: { data: 'sss' } });

                await adapter.delete({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, '2015-01-14/test1') });
                const res1 = await adapter.list({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, '2015-01-14/test1') });
                expect(res1.length).to.equal(0);

                await adapter.delete({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, '2015-01-14/test2') });
                const res2 = await adapter.list({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, '2015-01-14/test2') });
                expect(res2.length).to.equal(0);

                await adapter.delete({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, '2015-01-14/test3') });
                const res3 = await adapter.list({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, '2015-01-14/test3') });
                expect(res3.length).to.equal(0);
            }).timeout(80000);
            it(`delete by date more than 3000 items ${o.binary ? 'binary' : 'json'}`, async () => {
                const promiseArray = [];
                for (let i = 0; i < 3500; i += 1) {
                    promiseArray.push(adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, moment('2014-11-28').format(DateFormat), 'test3', `test${i}.json`), data: { data: 'sss' } }));
                }
                await Promise.all(promiseArray);
                await adapter.delete({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, '2014-11-28') });
                const res2 = await adapter.list({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, '2014-11-28') });
                expect(res2.length).to.equal(0);
            }).timeout(80000);
            it(`list objects without prefix ${o.binary ? 'binary' : 'json'}`, async () => {
                const jobId = Date.now().toString();
                await Promise.all([
                    adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, moment().format(DateFormat), jobId, '0'), data: 'test0' }),
                    adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, moment().format(DateFormat), jobId, '0'), data: 'test0' }),
                    adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE_RESULTS, `${o.binary ? 'binary' : 'json'}`, moment().format(this.DateFormat), jobId, 'result.json'), data: 'test0' }),
                    adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE_RESULTS, `${o.binary ? 'binary' : 'json'}`, moment().format(this.DateFormat), jobId, 'result.json'), data: 'test6' })]);
                const res1 = await adapter.list({ path: path.join(BUCKETS_NAMES.HKUBE, `${o.binary ? 'binary' : 'json'}`, '/') });
                expect(res1.length > 0).to.be.true;
                const res2 = await adapter.list({ path: path.join(BUCKETS_NAMES.HKUBE_RESULTS, `${o.binary ? 'binary' : 'json'}`, '/') });
                expect(res2.length > 0).to.be.true;
            }).timeout(80000);
            it(`list objects with delimiter ${o.binary ? 'binary' : 'json'}`, async () => {
                const jobId = Date.now().toString();
                await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE_INDEX, '2019-01-01', jobId, '0'), data: { data: 'sss1' } });
                await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE_INDEX, '2019-01-02', jobId, '1'), data: { data: 'sss2' } });
                await adapter.put({ path: path.join(BUCKETS_NAMES.HKUBE_INDEX, '2019-01-03', jobId, '2'), data: { data: 'sss3' } });

                const rd = await adapter.listPrefixes({ path: BUCKETS_NAMES.HKUBE_INDEX });
            expect(rd.includes('2019-01-01')).to.be.true;
            expect(rd.includes('2019-01-02')).to.be.true;
            expect(rd.includes('2019-01-03')).to.be.true;
        }).timeout(80000);
    });
});
})