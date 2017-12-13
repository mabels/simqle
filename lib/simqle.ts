import * as rx from 'rxjs';
// import * as RxMe from 'rxme';
import * as RxMe from 'rxme';

export const State = {
  OK: 'OK',
  ERROR: 'ERROR'
};

export class QEntryRun {
  public started: Date;
  public _completed: Date;
  public _error: any;

  constructor() {
    this.started = new Date();
  }

  public error(err: any): void {
    this._error = err;
  }

  public completed(): void {
    this._completed = new Date();
  }
}

export class QEntry<T> {
  public created: Date;
  public runs: QEntryRun[];
  public input: RxMe.Observable<T>;
  public output: RxMe.Subject<T>;

  constructor(input: RxMe.Observable<T>, output: RxMe.Subject<T>) {
    this.runs = [];
    this.input = input;
    this.output = output;
    this.created = new Date();
  }

  public isCompleted(): boolean {
    return !!this.runs.find(r => !!r.completed);
  }

  public isWaiting(): boolean {
    // console.log(this.runs);
    return this.runs.length == 0 || !!this.runs.find(qer => !qer.completed);
  }

  public newQEntryRun(): QEntryRun {
    const qer = new QEntryRun();
    this.runs.push(qer);
    return qer;
  }
}

export interface QConfig {
  taskTimer?: number;
  reclaimTimeout?: number;
  retryWaitTime?: number;
  maxExecuteCnt?: number;
}

export interface QWorker<T> {
  (input: RxMe.Observable<T>, output: RxMe.Subject<T>): void;
}

export class WorkerState<T> {
  private running: QEntryRun;
  private output: RxMe.Subject<T>;
  private readonly qWorker: QWorker<T>;
  private readonly q: Queue<T>;
  public readonly objectId: string;

  constructor(q: Queue<T>, qw: QWorker<T>) {
    this.q = q;
    this.qWorker = qw;
    this.running = null;
    this.objectId = ('' + (1000000000 + ~~(Math.random() * 1000000000))).slice(1);
  }

  public isFree(): boolean {
    return !this.running;
  }

  public run(qe: QEntry<any>): void {
    this.running = qe.newQEntryRun();
    this.output = new RxMe.Subject<T>(null);
    this.output.subscribe(
      (a) => {
        // console.log('WorkerState:next', a);
        qe.output.next(a);
      },
      (err) => {
        // console.log('WorkerState:error', err);
        this.running.error(err);
        this.running = null;
        qe.output.error(err);
      },
      () => {
        // console.log('WorkerState:completed');
        this.running.completed();
        this.running = null;
        qe.output.complete();
        this.q.processMemoryQ();
      }
    );
    this.qWorker(qe.input, this.output);
  }
}

// export class QueueSubject<T> extends RxMe.Subject<Queue<T>> { }
export class Subject<T> extends RxMe.Subject<Queue<T>> {
  constructor(a: any) {
    super(a);
  }
}

// export interface QueueObserver<T> extends RxMe.Observer<Queue<T>> { }
export interface Observer<T> extends RxMe.Observer<Queue<T>> { }

// export class QueueObservable<T> extends RxMe.Observable<Queue<T>> { }
export class Observable<T> extends RxMe.Observable<Queue<T>> { }

export class Logger<T> {
  private readonly upStream: Subject<T>;
  private readonly base: string[];
  constructor(upStream: Subject<T>, qid: number) {
    this.upStream = upStream;
    this.base = [`Q[${qid}]:`];
  }
  public info(...args: any[]): void {
    this.upStream.nextLog.info(this.base.concat(args));
  }
  public error(...args: any[]): void {
    this.upStream.nextLog.error(this.base.concat(args));
  }
  public debug(...args: any[]): void {
    this.upStream.nextLog.debug(this.base.concat(args));
  }
}

export class Queue<T> {
  // public q: Rx.Subject<QEntry<T>>;
  // public readonly deadLetter: Rx.Subject<QEntry<T>>;
  public qEntries: QEntry<any>[] = [];
  // public inProcess: boolean;
  public qTask: number;
  public reclaimTimeout: number;
  public taskTimer: number;
  public retryWaitTime: number;
  public maxExecuteCnt: number;
  public readonly workers: WorkerState<T>[];
  private readonly lengthSubject: rx.Subject<number>;
  // private readonly obs: QueueObserver<T>;
  public readonly logger: Logger<T>;

  constructor(obs: Subject<T>, argv: QConfig, id: number) {
    // this.inProcess = false;
    // this.obs = obs;
    this.logger = new Logger(obs, id);
    this.lengthSubject = new rx.Subject<number>();
    this.taskTimer = argv.taskTimer || 500;
    this.reclaimTimeout = argv.reclaimTimeout || 10000;
    this.retryWaitTime = argv.retryWaitTime || 1000;
    this.maxExecuteCnt = argv.maxExecuteCnt || 10;
    this.workers = [];
    // this.q = new Rx.Subject<QEntry<T>>();
    // this.q.subscribe(this.action.bind(this));
    // this.deadLetter = new Rx.Subject<QEntry<T>>();
    this.qTask = setInterval(this.processMemoryQ.bind(this), this.taskTimer);
  }

  public addWorker(qw: QWorker<T>): Queue<T> {
    const ws = new WorkerState(this, qw);
    this.workers.push(ws);
    this.logger.info(`added Worker:[${ws.objectId}]`);
    return this;
  }

  public length(): rx.Subject<number> {
    return this.lengthSubject;
  }

  public stop(): RxMe.Observable<void> {
    this.logger.info('run stop');
    // console.log('run stop');
    return RxMe.Observable.create(null, (observer: RxMe.Observer<void>) => {
      this.logger.info('queue stop');
      let forceStopAfter = 3;
      const action = () => {
        if (forceStopAfter <= 0 || this.qEntries.length == 0) {
          clearInterval(this.qTask);
          this.logger.info('Q Task stopped');
          observer.next(RxMe.done(true));
          observer.complete();
        } else {
          --forceStopAfter;
          this.logger.info('waiting for Q Task to stop', this.qEntries.length, forceStopAfter);
          setTimeout(action, this.taskTimer);
        }
      };
      action();
    });
  }

  // private action(qe: QEntry<T>): void {
  //   this.logger.debug('action:', qe);
  //   // qe.running = new Date();
  //   // ++qe.executeCnt;
  //   // qe.task.subscribe((_qe: T) => {
  //   //   qe.running = null;
  //   //   qe.completed = new Date();
  //   //   this.logger.debug('action:completed:', qe);
  //   // }, (error: any) => {
  //   //   qe.running = null;
  //   //   qe.retryAt = new Date((new Date()).getTime() + this.retryWaitTime);
  //   //   this.logger.debug('action:error:', qe.created.getTime(),
  //   //                                      qe.retryAt.getTime(),
  //   //                                      qe);
  //   // });
  // }

  public processMemoryQ(): void {
    const freeWorker = this.workers.find(w => w.isFree());
    const qentry = this.qEntries.find(q => q.isWaiting());
    if (freeWorker && qentry) {
      // this.logger.info('processMemoryQ:run');
      freeWorker.run(qentry);
    }
    const prevLen = this.qEntries.length;
    this.qEntries = this.qEntries.filter(a => !a.isCompleted());
    if (prevLen != this.qEntries.length) {
      this.lengthSubject.next(this.qEntries.length);
    }
    // this.qEntries = this.qEntries.filter((qe) => {
    //   if (qe.completed) {
    //     this.logger.debug('completed remove', qe);
    //     return false;
    //   } else if (qe.executeCnt >= this.maxExecuteCnt) {
    //     this.logger.error('drop queue entry:', qe);
    //     this.deadLetter.next(qe);
    //     return false;
    //   } else if (qe.retryAt) {
    //     if (now >= qe.retryAt.getTime()) {
    //       qe.retryAt = null;
    //       this.logger.debug('retry queue entry:', qe);
    //       this.q.next(qe);
    //     }
    //     return true;
    //   } else if (!qe.running) {
    //     this.logger.debug('trigger action:', qe);
    //     this.q.next(qe);
    //   } else if ((qe.running && qe.executeCnt < this.maxExecuteCnt &&
    //     (now - qe.running.getTime()) >= this.reclaimTimeout)) {
    //     qe.running = null;
    //     this.logger.debug('retry execution time entry:', qe);
    //     this.q.next(qe);
    //   }
    //   return true;
    // });
  }

  public push<Y>(action: RxMe.Observable<Y>, completed: RxMe.Subject<Y>): void {
    this.qEntries.push(new QEntry<Y>(action, completed));
    this.lengthSubject.next(this.qEntries.length);
    this.processMemoryQ();
  }

}

let queueId = ~~(0x1000000 * Math.random());
export function start<T>(argv: QConfig): Observable<T> {
  return Observable.create(Queue, (obs: Subject<T>) => {
    const q = RxMe.data(new Queue<T>(obs, argv, queueId++));
    q.asKind<Queue<T>>().logger.info('Queue Started');
    obs.next(q);
  });
}

export default Queue;
