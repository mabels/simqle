import { assert } from 'chai';
// import * as queue from '../lib/simqle';
import * as RxMe from 'rxme';
import * as simqle from '../lib/simqle';
// import { Subject } from 'rxme';
// import * as rx from 'rxjs';
// import * as Winston from 'winston';
// import { Promise } from 'es6-shim';
// import { Promise } from 'es6-shim';

// const logger = new Rx.Subject<queue.LogMsg>();
// const winstonLogger = new Winston.Logger({
//   level: 'info',
//   transports: [
//     new (Winston.transports.Console)()
//   ]
// });

function logger(l: RxMe.LogMsg): void {
  const awin = console as any;
  awin[l.level].apply(awin[l.level], l.parts);
}

function counter(oneState: any): RxMe.Observer<number> {
  return {
    next: (a: RxMe.RxMe<number>) => {
      // console.log('one.Next:', a, oneState.next);
      oneState.next += a.asKind<number>();
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

function worker(ms: number, ofs: number, cb: () => void): simqle.QWorker<number> {
  return (input: RxMe.Observable<number>, output: RxMe.Subject<number>) => {
    // console.log('Worker:Started');
    const nrs: number[] = [];
    input.match((done: RxMe.Subject<number>, nr: number) => {
      // console.log('worker:match', nr);
      nrs.push(nr);
      return true;
    }).matchComplete((res: RxMe.Subject<any>, done: RxMe.Done) => {
      // console.log('worker:done');
      setTimeout(() => {
        // console.log('worker:done:Timeout:', ofs, nrs);
        nrs.forEach(nr => output.next(RxMe.data(nr + ofs)));
        output.complete();
        res.done(true);
        cb();
      }, ms);
      // output.next(RxMe.data(done));
      // output.data(done);
      // cb();
      return res;
    }).passTo(output);
    /*
      -    }, (err: any) => {
      -      output.error(err);
      -    }, () => {
      -      setTimeout(() => {
      -        nrs.forEach(nr => output.next(nr + ofs));
      -        output.complete();
      -        cb();
      -      }, ms);
    */

  };
}

function qLoop(q: simqle.Queue<number>, loops: number, countDownLedge: RxMe.Subject<number>): Promise<void> {
  return new Promise<void>((rs, rj) => {
    const oneState = { next: 0, error: 0, complete: 0 };
    const one = counter(oneState);

    countDownLedge.subscribe((a) => {
      try {
        if (a.asKind<number>() == loops) {
          assert.equal(oneState.next, ((loops * (loops + 1)) / 2));
          assert.equal(oneState.error, 0);
          assert.equal(oneState.complete, loops);
          q.stop().matchDone(() => { rs(); return false; }).passTo();
        }
      } catch (e) {
        q.stop().matchDone(() => { rj(e); return false; }).passTo();
      }
    });

    for (let i = 1; i <= loops; ++i) {
      // console.log('simqle:', i);
      const c1000 = new RxMe.Subject<number>(RxMe.Match.NUMBER);
      c1000.subscribe(one);
      // console.log(`QEntry:Off:${i}`);
      q.push<number>(RxMe.Observable.create(RxMe.Match.NUMBER, (obs: RxMe.Observer<number>) => {
        // console.log(`QEntry:${j}`);
        obs.next(RxMe.data(i));
        obs.complete();
      }), c1000);
    }
  });
}

describe('queue', () => {

  it('simple no worker', async (): Promise<void> => {
    // simple
    return new Promise<void>((rs, rj) => {
      // console.log('--1');
      simqle.start<number>({ taskTimer: 50 })
        .matchLogMsg((_: simqle.Subject<any>, log: RxMe.LogMsg) => {
          // console.log('LogMsg:', log);
          logger(log);
          return false;
        }).match((_: simqle.Subject<any>, q: simqle.Queue<number>) => {
          // console.log('Match:', q);
          const oneState = { next: 0, error: 0, complete: 0 };
          q.length().subscribe((a: number) => {
            // console.log('>>>>', a);
            try {
              if (a >= 3) {
                assert.equal(a, 3);
                assert.equal(oneState.next, 0);
                assert.equal(oneState.error, 0);
                assert.equal(oneState.complete, 0);
                q.stop().matchDone(() => { rs(); return false; }).passTo();
              }
            } catch (e) {
              q.stop().matchDone(() => { rj(e); return false; }).passTo();
            }
          });
          const one = counter(oneState);
          const c1000 = new RxMe.Subject<number>(RxMe.Match.NUMBER);
          c1000.subscribe(one);
          q.push<number>(RxMe.Observable.create(RxMe.Match.NUMBER, (obs: RxMe.Observer<number>) => {
            obs.next(RxMe.data(1000));
            obs.complete();
          }), c1000);
          const c2000 = new RxMe.Subject<number>(RxMe.Match.NUMBER);
          c2000.subscribe(one);
          q.push<number>(RxMe.Observable.create(RxMe.Match.NUMBER, (obs: RxMe.Observer<number>) => {
            obs.next(RxMe.data(2000));
            obs.complete();
          }), c2000);
          const c3000 = new RxMe.Subject<number>(RxMe.Match.NUMBER);
          c3000.subscribe(one);
          q.push<number>(RxMe.Observable.create(RxMe.Match.NUMBER, (obs: RxMe.Observer<number>) => {
            obs.next(RxMe.data(3000));
            obs.complete();
          }), c3000);
          return false;
        }).passTo();
    });
  });

  it('simple one worker', async () => {
    // simple
    return new Promise((rs, rj) => {
      simqle.start<number>({ taskTimer: 50 })
        .matchLogMsg((_: any, rlm: RxMe.LogMsg) => {
          logger(rlm);
          return true;
        }).match((done: simqle.Subject<any>, q: simqle.Queue<number>) => {
          // console.log(q);
          let countDown = 0;
          const countDownLedge = new RxMe.Subject<number>(RxMe.Match.NUMBER);
          q.addWorker(worker(0, 2, () => countDownLedge.next(RxMe.data(++countDown))));

          const oneState = { next: 0, error: 0, complete: 0 };
          const one = counter(oneState);
          countDownLedge.subscribe((a) => {
            try {
              // console.log('Wait:', a);
              if (a.asKind<number>() == 3) {
                assert.equal(oneState.next, 6006);
                assert.equal(oneState.error, 0);
                assert.equal(oneState.complete, 3);
                q.stop().matchDone(() => { rs(); return false; }).passTo();
              }
            } catch (e) {
              q.stop().matchDone(() => { rj(e); return false; }).passTo();
            }
          });

          const c1000 = new RxMe.Subject<number>(RxMe.Match.NUMBER);
          c1000.subscribe(one);
          q.push<number>(RxMe.Observable.create(RxMe.Match.NUMBER, (obs: RxMe.Observer<number>) => {
            // console.log('QEntry:1000');
            obs.next(RxMe.data(1000));
            obs.complete();
          }), c1000);
          const c2000 = new RxMe.Subject<number>(RxMe.Match.NUMBER);
          c2000.subscribe(one);
          q.push<number>(RxMe.Observable.create(RxMe.Match.NUMBER, (obs: RxMe.Observer<number>) => {
            // console.log('QEntry:2000');
            obs.next(RxMe.data(2000));
            obs.complete();
          }), c2000);
          const c3000 = new RxMe.Subject<number>(RxMe.Match.NUMBER);
          c3000.subscribe(one);
          q.push<number>(RxMe.Observable.create(RxMe.Match.NUMBER, (obs: RxMe.Observer<number>) => {
            // console.log('QEntry:3000');
            obs.next(RxMe.data(3000));
            obs.complete();
          }), c3000);
          return done;
        }).passTo();
    });
  });

  it('1000 one worker', async function (): Promise<void> {
    // this.timeout(10000);
    // simple
    return new Promise<void>((rs, rj) => {
      simqle.start<number>({ taskTimer: 50 }).matchLogMsg((_: simqle.Subject<any>, rsq: RxMe.LogMsg): boolean => {
        logger(rsq);
        return false;
      }).match((obs: simqle.Subject<any>, q: simqle.Queue<number>) => {
        // console.log(`starting 1000`);
        let countDown = 0;
        const countDownLedge = new RxMe.Subject<number>(RxMe.Match.NUMBER);
        q.addWorker(worker(0, 0, () => countDownLedge.next(RxMe.data(++countDown))));
        qLoop(q, 1000, countDownLedge).then(() => {
          obs.done(true);
          rs();
        }).catch(() => {
          obs.done(true);
          rj();
        });
        return obs;
      }).passTo();
    });
  });

  it('simple two worker', async () => {
    return new Promise((rs, rj) => {
      const workers = [0, 0];
      simqle.start<number>({ taskTimer: 50 })
        .matchLogMsg((_: simqle.Subject<any>, rsq: RxMe.LogMsg) => {
          logger(rsq);
          return false;
        }).match((done: simqle.Subject<any>, q: simqle.Queue<number>) => {
          let countDown = 0;
          const countDownLedge = new RxMe.Subject<number>(RxMe.Match.NUMBER);
          q.addWorker(worker(10, 2, () => { workers[0]++; countDownLedge.next(RxMe.data(++countDown)); }));
          q.addWorker(worker(15, 2, () => { workers[1]++; countDownLedge.next(RxMe.data(++countDown)); }));
          const oneState = { next: 0, error: 0, complete: 0 };
          const one = counter(oneState);
          countDownLedge.subscribe((a) => {
            try {
              // console.log('Wait:', a);
              if (a.asKind<number>() == 3) {
                assert.equal(workers[0], 2, `fast worker count ${workers}`);
                assert.equal(workers[1], 1, 'slow worker count');
                assert.equal(oneState.next, 6006);
                assert.equal(oneState.error, 0);
                assert.equal(oneState.complete, 3);
                q.stop().matchDone(() => { rs(); return false; }).passTo();
              }
            } catch (e) {
              q.stop().matchDone(() => { rj(); return false; }).passTo();
            }
          });

          const c1000 = new RxMe.Subject<number>(RxMe.Match.NUMBER);
          c1000.subscribe(one);
          q.push<number>(RxMe.Observable.create(RxMe.Match.NUMBER, (obs: RxMe.Observer<number>) => {
            // console.log('QEntry:1000');
            obs.next(RxMe.data(1000));
            obs.complete();
          }), c1000);
          const c2000 = new RxMe.Subject<number>(RxMe.Match.NUMBER);
          c2000.subscribe(one);
          q.push<number>(RxMe.Observable.create(RxMe.Match.NUMBER, (obs: RxMe.Observer<number>) => {
            // console.log('QEntry:2000');
            obs.next(RxMe.data(2000));
            obs.complete();
          }), c2000);
          const c3000 = new RxMe.Subject<number>(RxMe.Match.NUMBER);
          c3000.subscribe(one);
          q.push<number>(RxMe.Observable.create(RxMe.Match.NUMBER, (obs: RxMe.Observer<number>) => {
            // console.log('QEntry:3000');
            obs.next(RxMe.data(3000));
            obs.complete();
          }), c3000);
          return done;
        }).passTo();
    });
  });

  it('1000 three worker', async function (): Promise<void> {
    // this.timeout(10000);
    // simple
    return new Promise<void>((rs, rj) => {
      simqle.start<number>({ taskTimer: 50 })
        .matchLogMsg((_: simqle.Subject<any>, rsq: RxMe.LogMsg) => {
          logger(rsq);
          return false;
        }).match((obs: simqle.Subject<any>, q: simqle.Queue<number>) => {
          let countDown = 0;
          const countDownLedge = new RxMe.Subject<number>(RxMe.Match.NUMBER);
          q.addWorker(worker(0, 0, () => countDownLedge.next(RxMe.data(++countDown))));
          q.addWorker(worker(0, 0, () => countDownLedge.next(RxMe.data(++countDown))));
          q.addWorker(worker(0, 0, () => countDownLedge.next(RxMe.data(++countDown))));
          qLoop(q, 1000, countDownLedge).then(() => {
            obs.done(true);
            rs();
          }).catch(() => {
            obs.done(true);
            rj();
          });
          return obs;
        }).passTo();
    });
  });

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
      q.stop().subscribe(() => { done(); });
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
