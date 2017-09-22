import { assert } from 'chai';
import * as queue from '../lib/simqle';
import * as rxjs from 'rxjs';
import * as Rx from 'urxjs/dist/lib/abstract-rx';
import * as Winston from 'winston';

Rx.inject(rxjs);

const logger = new Rx.Subject<queue.LogMsg>();
const winstonLogger = new Winston.Logger({
  level: 'info',
  transports: [
    new (Winston.transports.Console)()
  ]
});
logger.subscribe((l: queue.LogMsg) => {
  const awin = winstonLogger as any;
  awin[l.level].apply(awin[l.level], l.parts);
});

describe('queue', () => {

  it.only('simple', (done) => {
    // simple
    const q = queue.start<number>(logger, {});
    let calls = 10;
    let pulls = 10;
    let pidx = 0;
    q.q.subscribe(data => {
      console.log('task: idx:pulls', pulls);
      --pulls;
      data.task.subscribe(idx => {
        console.log('task: idx', idx, pidx++);
        assert.equal(idx, pidx++);
      }, err => {
        console.log('task: idx: error', err);
      }, () => {
        console.log('task: idx: complete');
      });
    });
    (Array(calls).fill(0)).forEach((_: any, idx: number) => {
      // console.log('simple:', idx);
      q.push(Rx.Observable.create((obs: Rx.Observer<number>) => {
        console.log('task: vals', calls, idx);
        --calls;
        obs.next(idx);
        obs.complete();
      }));
    });
    setTimeout(() => {
      assert.equal(0, calls);
      assert.equal(0, pulls);
      q.stop().subscribe(() => {
        done();
      });
    }, 800);
  });

  it('retries', (done) => {
    // simple 2 retries than ok with measure retryWaittime
    const q = queue.start<number>(logger, {
      reclaimTimeout: 100,
      retryWaitTime: 50,
      maxExecuteCnt: 5,
      taskTimer: 10
    });
    const idxTimer = new Map<number, number[]>();
    let calls = 10;
    const retrys = 2;
    (Array(calls).fill(0)).forEach((_: any, idx: number) => {
      // console.log('simple:', idx);
      idxTimer.set(idx, []);
      let cnt = 0;
      q.push(Rx.Observable.create((obs: Rx.Observer<number>) => {
        idxTimer.get(idx).push((new Date()).getTime());
        if (cnt++ < retrys) {
          obs.error(idx);
        } else {
          obs.next(idx);
          obs.complete();
        }
      }));
    });
    let deadLetters = 0;
    q.deadLetter.subscribe(() => { ++deadLetters; });
    setTimeout(() => {
      assert.equal(0, deadLetters, 'calls != deadletter');
      idxTimer.forEach((v, k) => {
        assert.equal(retrys + 1, v.length);
        const diff = v.map((x, i, vas) => {
          if (i < 1) { return 0; }
          return x - vas[i - 1];
        }).slice(1);
        // console.log('diff:', diff, v);
        diff.forEach((d) => {
          assert.isTrue(q.retryWaitTime <= d && d <= (q.retryWaitTime * 1.5));
        });
      });
      // assert.equal(0, calls);
      q.stop().subscribe(done);
    }, 400);
  });

  it('reclaim', (done) => {
    // simple 2 retries than ok with measure retryWaittime
    const q = queue.start<number>(logger, {
      reclaimTimeout: 100,
      retryWaitTime: 50,
      maxExecuteCnt: 5,
      taskTimer: 10
    });
    const idxTimer = new Map<number, number[]>();
    let calls = 10;
    const retrys = 2;
    (Array(calls).fill(0)).forEach((_: any, idx: number) => {
      // console.log('simple:', idx);
      idxTimer.set(idx, []);
      let cnt = 0;
      q.push(Rx.Observable.create((obs: Rx.Observer<number>) => {
        idxTimer.get(idx).push((new Date()).getTime());
        if (cnt++ >= retrys) {
          obs.next(idx);
        }
      }));
    });
    let deadLetters = 0;
    q.deadLetter.subscribe(() => { ++deadLetters; });
    setTimeout(() => {
      assert.equal(0, deadLetters, 'calls != deadletter');
      idxTimer.forEach((v, k) => {
        assert.equal(retrys + 1, v.length);
        const diff = v.map((x, i, vas) => {
          if (i < 1) { return 0; }
          return x - vas[i - 1];
        }).slice(1);
        // console.log('diff:', diff, v);
        diff.forEach((d) => {
          assert.isTrue(q.reclaimTimeout <= d && d <= (q.reclaimTimeout * 1.5));
        });
      });
      // assert.equal(0, calls);
      q.stop().subscribe(done);
    }, 500);
  });

  it('error', (done) => {
    // simple 5 error remove
    const q = queue.start<number>(logger, {
      reclaimTimeout: 100,
      retryWaitTime: 50,
      maxExecuteCnt: 5,
      taskTimer: 10
    });
    const idxTimer = new Map<number, number[]>();
    let calls = 10;
    (Array(calls).fill(0)).forEach((_: any, idx: number) => {
      // console.log('simple:', idx);
      idxTimer.set(idx, []);
      q.push(Rx.Observable.create((obs: Rx.Observer<number>) => {
        // --calls;
        idxTimer.get(idx).push((new Date()).getTime());
        obs.error(idx);
        // obs.complete();
      }));
    });
    let deadLetters = 0;
    q.deadLetter.subscribe(() => { ++deadLetters; });
    setTimeout(() => {
      assert.equal(calls, deadLetters, 'calls != deadletter');
      idxTimer.forEach((v, k) => {
        assert.equal(q.maxExecuteCnt, v.length);
        const diff = v.map((x, i, vas) => {
          if (i < 1) { return 0; }
          return x - vas[i - 1];
        }).slice(1);
        // console.log('diff:', diff, v);
        diff.forEach((d) => {
          assert.isTrue(q.retryWaitTime <= d && d <= (q.retryWaitTime * 1.5));
        });
      });
      // assert.equal(0, calls);
      q.stop().subscribe(done);
    }, 800);
  });

});
