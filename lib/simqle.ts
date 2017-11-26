import * as Rx from 'rxjs';

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
  public input: Rx.Observable<T>;
  public output: Rx.Subject<T>;

  constructor(input: Rx.Observable<T>, output: Rx.Subject<T>) {
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

export interface LogMsg {
  level: string;
  parts: any[];
}

export interface QWorker<T> {
  (input: Rx.Observable<T>, output: Rx.Subject<T>): void;
}

export class WorkerState<T> {
  private running: QEntryRun;
  private output: Rx.Subject<T>;
  private readonly qWorker: QWorker<T>;
  private readonly q: Queue<T>;

  constructor(q: Queue<T>, qw: QWorker<T>) {
    this.q = q;
    this.qWorker = qw;
    this.running = null;
  }
  public isFree(): boolean {
    return !this.running;
  }

  public run(qe: QEntry<any>): void {
    this.running = qe.newQEntryRun();
    this.output = new Rx.Subject();
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

class Logger {
  private upStream: Rx.Subject<LogMsg>;
  constructor(upStream: Rx.Subject<LogMsg>) {
    this.upStream = upStream;
  }
  public info(...args: any[]): void {
    this.upStream.next({ level: 'info', parts: args});
  }
  public error(...args: any[]): void {
    this.upStream.next({ level: 'error', parts: args});
  }
  public debug(...args: any[]): void {
    this.upStream.next({ level: 'debug', parts: args});
  }
}

export class Queue<T> {
  public q: Rx.Subject<QEntry<T>>;
  public deadLetter: Rx.Subject<QEntry<T>>;
  // public q: Rx.Observable<Entry>;
  public qEntries: QEntry<any>[] = [];
  public inProcess: boolean;
  public qTask: number;
  public reclaimTimeout: number;
  public taskTimer: number;
  public retryWaitTime: number;
  public maxExecuteCnt: number;
  public workers: WorkerState<T>[];
  private logger: Logger;
  private lengthSubject: Rx.Subject<number>;

  constructor(logger: Rx.Subject<LogMsg>, argv: QConfig) {
    this.inProcess = false;
    this.lengthSubject = new Rx.Subject();
    this.taskTimer = argv.taskTimer || 500;
    this.reclaimTimeout = argv.reclaimTimeout || 10000;
    this.retryWaitTime = argv.retryWaitTime || 1000;
    this.maxExecuteCnt = argv.maxExecuteCnt || 10;
    this.workers = [];
    this.logger = new Logger(logger);
    this.q = new Rx.Subject<QEntry<T>>();
    this.deadLetter = new Rx.Subject<QEntry<T>>();
    this.q.subscribe(this.action.bind(this));
    this.qTask = setInterval(this.processMemoryQ.bind(this), this.taskTimer);
  }

  public addWorker(qw: QWorker<T>): Queue<T> {
    this.workers.push(new WorkerState(this, qw));
    return this;
  }

  public length(): Rx.Subject<number> {
    return this.lengthSubject;
  }

  public stop(): Rx.Observable<void> {
    this.logger.info('run stop');
    // console.log('run stop');
    return Rx.Observable.create((observer: Rx.Observer<void>) => {
      this.logger.info('queue stop');
      const action = () => {
        if (this.qEntries.length == 0) {
          this.logger.info('Q Task stopped');
          clearInterval(this.qTask);
          observer.next(null);
          observer.complete();
        } else {
          this.logger.info('waiting for Q Task to stop', this.qEntries.length);
          setTimeout(action, this.taskTimer);
        }
      };
      action();
    });
  }

  private action(qe: QEntry<T>): void {
    this.logger.debug('action:', qe);
    // qe.running = new Date();
    // ++qe.executeCnt;
    // qe.task.subscribe((_qe: T) => {
    //   qe.running = null;
    //   qe.completed = new Date();
    //   this.logger.debug('action:completed:', qe);
    // }, (error: any) => {
    //   qe.running = null;
    //   qe.retryAt = new Date((new Date()).getTime() + this.retryWaitTime);
    //   this.logger.debug('action:error:', qe.created.getTime(),
    //                                      qe.retryAt.getTime(),
    //                                      qe);
    // });
  }

  public processMemoryQ(): void {
    const freeWorker = this.workers.find(w => w.isFree());
    const qentry = this.qEntries.find(q => q.isWaiting());
    if (freeWorker && qentry) {
      // this.logger.info('processMemoryQ:run');
      freeWorker.run(qentry);
    }
    this.qEntries = this.qEntries.filter(a => !a.isCompleted());
    this.lengthSubject.next(this.qEntries.length);
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

  public push<Y>(action: Rx.Observable<Y>, completed: Rx.Subject<Y>): void {
    this.qEntries.push(new QEntry<Y>(action, completed));
    this.lengthSubject.next(this.qEntries.length);
    this.processMemoryQ();
  }

}

export function start<T>(logger: Rx.Subject<LogMsg>, argv: any): Queue<T> {
  return new Queue<T>(logger, argv);
}

export default Queue;
