import { assert } from 'chai';
import * as queue from '../lib/simqle';
import * as Rx from 'rxjs';
import * as Winston from 'winston';
// import { Promise } from 'es6-shim';

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

function counter(oneState: any): Rx.Observer<number> {
  return {
    next: (a: number) => {
      // console.log('one.Next:', a, oneState.next);
      oneState.next += a;
    },
    error: (err: any) => {
      oneState.error++;
      // console.log('one.Error:', err);
    },
    complete: () => {
      oneState.complete++;
      // console.log('one.Completed:');
    }
  };
}

function worker(ms: number, ofs: number, cb: () => void): queue.QWorker<number> {
  return (input: Rx.Observable<number>, output: Rx.Subject<number>) => {
    // console.log('Worker:Started');
    const nrs: number[] = [];
    input.subscribe((nr: number) => {
      // console.log('worker:', nr);
      nrs.push(nr);
    }, (err: any) => {
      output.error(err);
    }, () => {
      setTimeout(() => {
        nrs.forEach(nr => output.next(nr + ofs));
        output.complete();
        cb();
      }, ms);
    });
  };
}

function qLoop(q: queue.Queue<number>, loops: number, countDownLedge: Rx.Subject<number>): Promise<void> {
  return new Promise<void>((rs, rj) => {
    const oneState = { next: 0, error: 0, complete: 0 };
    const one = counter(oneState);

    countDownLedge.subscribe((a) => {
      try {
        if (a == loops) {
          assert.equal(oneState.next, ((loops * (loops + 1)) / 2));
          assert.equal(oneState.error, 0);
          assert.equal(oneState.complete, loops);
          rs();
        }
      } catch (e) {
        rj(e);
      }
    });

    for (let i = 1; i <= loops; ++i) {
      // console.log('simqle:', i);
      const c1000 = new Rx.Subject<number>();
      c1000.subscribe(one);
      // console.log(`QEntry:Off:${i}`);
      q.push<number>(Rx.Observable.create((obs: Rx.Observer<number>) => {
        // console.log(`QEntry:${j}`);
        obs.next(i);
        obs.complete();
      }), c1000);
    }
  });
}

describe('queue', () => {

  it('simple no worker', async () => {
    // simple
    const q = queue.start<number>(logger, { taskTimer: 50 });
    // q.addWorker(() => { setTimeout(); });
    // q.addWorker(() => { setTimeout(); });
    const oneState = { next: 0, error: 0, complete: 0 };
    const one = counter(oneState);
    const c1000 = new Rx.Subject<number>();
    c1000.subscribe(one);
    q.push<number>(Rx.Observable.create((obs: Rx.Observer<number>) => {
      obs.next(1000);
      obs.complete();
    }), c1000);
    const c2000 = new Rx.Subject<number>();
    c2000.subscribe(one);
    q.push<number>(Rx.Observable.create((obs: Rx.Observer<number>) => {
      obs.next(2000);
      obs.complete();
    }), c2000);
    const c3000 = new Rx.Subject<number>();
    c3000.subscribe(one);
    q.push<number>(Rx.Observable.create((obs: Rx.Observer<number>) => {
      obs.next(3000);
      obs.complete();
    }), c3000);

    return new Promise((rs, rj) => {
      q.length().subscribe((a: number) => {
        try {
          assert.equal(a, 3);
          assert.equal(oneState.next, 0);
          assert.equal(oneState.error, 0);
          assert.equal(oneState.complete, 0);
          rs();
        } catch (e) {
          rj(e);
        }
      });
    });
  });

  it('simple one worker', async () => {
    // simple
    return new Promise((rs, rj) => {
      const q = queue.start<number>(logger, { taskTimer: 50 });
      let countDown = 0;
      const countDownLedge = new Rx.Subject<number>();
      q.addWorker(worker(0, 2, () => countDownLedge.next(++countDown)));

      const oneState = { next: 0, error: 0, complete: 0 };
      const one = counter(oneState);
      countDownLedge.subscribe((a) => {
        try {
          // console.log('Wait:', a);
          if (a == 3) {
            assert.equal(oneState.next, 6006);
            assert.equal(oneState.error, 0);
            assert.equal(oneState.complete, 3);
            rs();
          }
        } catch (e) {
          rj(e);
        }
      });

      const c1000 = new Rx.Subject<number>();
      c1000.subscribe(one);
      q.push<number>(Rx.Observable.create((obs: Rx.Observer<number>) => {
        // console.log('QEntry:1000');
        obs.next(1000);
        obs.complete();
      }), c1000);
      const c2000 = new Rx.Subject<number>();
      c2000.subscribe(one);
      q.push<number>(Rx.Observable.create((obs: Rx.Observer<number>) => {
        // console.log('QEntry:2000');
        obs.next(2000);
        obs.complete();
      }), c2000);
      const c3000 = new Rx.Subject<number>();
      c3000.subscribe(one);
      q.push<number>(Rx.Observable.create((obs: Rx.Observer<number>) => {
        // console.log('QEntry:3000');
        obs.next(3000);
        obs.complete();
      }), c3000);

    });
  });

  it('1000 one worker', async function (): Promise<void> {
    // this.timeout(10000);
    // simple
    const q = queue.start<number>(logger, { taskTimer: 50 });
    let countDown = 0;
    const countDownLedge = new Rx.Subject<number>();
    q.addWorker(worker(0, 0, () => countDownLedge.next(++countDown)));
    return qLoop(q, 1000, countDownLedge);
  });

  it('simple two worker', async () => {
    return new Promise((rs, rj) => {
      const workers = [0, 0];
      const q = queue.start<number>(logger, { taskTimer: 50 });
      let countDown = 0;
      const countDownLedge = new Rx.Subject<number>();
      q.addWorker(worker(10, 2, () => { workers[0]++; countDownLedge.next(++countDown); }));
      q.addWorker(worker(15, 2, () => { workers[1]++; countDownLedge.next(++countDown); }));
      const oneState = { next: 0, error: 0, complete: 0 };
      const one = counter(oneState);
      countDownLedge.subscribe((a) => {
        try {
          // console.log('Wait:', a);
          if (a == 3) {
            assert.equal(workers[0], 2, `fast worker count ${workers}`);
            assert.equal(workers[1], 1, 'slow worker count');
            assert.equal(oneState.next, 6006);
            assert.equal(oneState.error, 0);
            assert.equal(oneState.complete, 3);
            rs();
          }
        } catch (e) {
          rj(e);
        }
      });

      const c1000 = new Rx.Subject<number>();
      c1000.subscribe(one);
      q.push<number>(Rx.Observable.create((obs: Rx.Observer<number>) => {
        // console.log('QEntry:1000');
        obs.next(1000);
        obs.complete();
      }), c1000);
      const c2000 = new Rx.Subject<number>();
      c2000.subscribe(one);
      q.push<number>(Rx.Observable.create((obs: Rx.Observer<number>) => {
        // console.log('QEntry:2000');
        obs.next(2000);
        obs.complete();
      }), c2000);
      const c3000 = new Rx.Subject<number>();
      c3000.subscribe(one);
      q.push<number>(Rx.Observable.create((obs: Rx.Observer<number>) => {
        // console.log('QEntry:3000');
        obs.next(3000);
        obs.complete();
      }), c3000);

    });
  });

  it('1000 three worker', async function (): Promise<void> {
    // this.timeout(10000);
    // simple
    const q = queue.start<number>(logger, { taskTimer: 50 });
    let countDown = 0;
    const countDownLedge = new Rx.Subject<number>();
    q.addWorker(worker(0, 0, () => countDownLedge.next(++countDown)));
    q.addWorker(worker(0, 0, () => countDownLedge.next(++countDown)));
    q.addWorker(worker(0, 0, () => countDownLedge.next(++countDown)));
    return qLoop(q, 1000, countDownLedge);
  });

  /*
    let calls = 10;
    let pulls = 10;
    let pidx = 0;
    q.q.subscribe(data => {
      // console.log('task: idx:pulls', pulls);
      --pulls;
      data.task.subscribe(idx => {
        // console.log('task: idx', idx, pidx);
        assert.equal(idx, pidx++, '-1-');
      }, err => {
        // console.log('task: idx: error', err);
      }, () => {
        // console.log('task: idx: complete');
      });
    });
    (Array(calls).fill(0)).forEach((_: any, idx: number) => {
      // console.log('simple:', idx);
      // const id = Math.random();
      --calls;
      q.push(Rx.Observable.create((obs: Rx.Observer<number>) => {
        // console.log('task: vals', id, calls, idx);
        obs.next(idx);
        obs.complete();
      }));
    });
    setTimeout(() => {
      assert.equal(0, pulls, 'pulls');
      assert.equal(0, calls, 'calls');
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
  */
});
