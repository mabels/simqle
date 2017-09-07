import * as Rx from 'rxjs';
import * as winston from 'winston';

export const State = {
  OK: 'OK',
  ERROR: 'ERROR'
};

export class QEntry<T> {
  public created: Date;
  public running: Date;
  public retryAt: Date;
  public completed: Date;
  public executeCnt: number;
  public task: Rx.Observable<QEntry<T>>;

  constructor(task: Rx.Observable<QEntry<T>>) {
    this.executeCnt = 0;
    this.task = task;
    this.created = new Date();
  }
}

export interface QConfig {
  taskTimer?: number;
  reclaimTimeout?: number;
  retryWaitTime?: number;
  maxExecuteCnt?: number;
}

export class Queue<T> {
  public q: Rx.Subject<QEntry<T>>;
  public deadLetter: Rx.Subject<QEntry<T>>;
  // public q: Rx.Observable<Entry>;
  public qEntries: QEntry<T>[] = [];
  public inProcess: boolean;
  public qTask: number;
  public reclaimTimeout: number;
  public taskTimer: number;
  public retryWaitTime: number;
  public maxExecuteCnt: number;
  public logger: winston.LoggerInstance;

  constructor(logger: winston.LoggerInstance, argv: QConfig) {
    this.inProcess = false;
    this.taskTimer = argv.taskTimer || 500;
    this.reclaimTimeout = argv.reclaimTimeout || 10000;
    this.retryWaitTime = argv.retryWaitTime || 1000;
    this.maxExecuteCnt = argv.maxExecuteCnt || 10;
    this.logger = logger;
    this.q = new Rx.Subject<QEntry<T>>();
    this.deadLetter = new Rx.Subject<QEntry<T>>();
    this.q.subscribe(this.action.bind(this));
    this.qTask = setInterval(this.processMemoryQ.bind(this), this.taskTimer);
    //   next: (e) => {
    //     console.log('observerA: ' + e);
    //   }
    // });
  }

  public stop(): Rx.Observable<void> {
    this.logger.info('run stop');
    console.log('run stop');
    return Rx.Observable.create((observer: Rx.Observer<void>) => {
      this.logger.info('queue stop');
      const action = () => {
        if (this.qEntries.length == 0) {
          this.logger.info('Q Task stopped');
          clearInterval(this.qTask);
          observer.next(null);
          observer.complete();
        } else {
          this.logger.debug('waiting for Q Task to stop');
          setTimeout(action, this.taskTimer);
        }
      };
      action();
    });
  }

  private action(qe: QEntry<T>): void {
    this.logger.debug('action:', qe);
    qe.running = new Date();
    ++qe.executeCnt;
    qe.task.subscribe((_qe: QEntry<T>) => {
      qe.running = null;
      qe.completed = new Date();
      this.logger.debug('action:completed:', qe);
    }, (error: any) => {
      qe.running = null;
      qe.retryAt = new Date((new Date()).getTime() + this.retryWaitTime);
      this.logger.debug('action:error:', qe.created.getTime(),
                                         qe.retryAt.getTime(),
                                         qe);
    });
  }

  private processMemoryQ(): void {
    if (this.inProcess) {
      return;
    }
    this.inProcess = true;
    let now = (new Date()).getTime();
    this.qEntries = this.qEntries.filter((qe) => {
      if (qe.completed) {
        this.logger.debug('completed remove', qe);
        return false;
      } else if (qe.executeCnt >= this.maxExecuteCnt) {
        this.logger.error('drop queue entry:', qe);
        this.deadLetter.next(qe);
        return false;
      } else if (qe.retryAt) {
        if (now >= qe.retryAt.getTime()) {
          qe.retryAt = null;
          this.logger.debug('retry queue entry:', qe);
          this.q.next(qe);
        }
        return true;
      } else if (!qe.running) {
        this.logger.debug('trigger action:', qe);
        this.q.next(qe);
      } else if ((qe.running && qe.executeCnt < this.maxExecuteCnt &&
        (now - qe.running.getTime()) >= this.reclaimTimeout)) {
        qe.running = null;
        this.logger.debug('retry execution time entry:', qe);
        this.q.next(qe);
      }
      return true;
    });
    this.inProcess = false;
  }

  public push(action: Rx.Observable<QEntry<T>>): void {
    this.qEntries.push(new QEntry(action));
    this.processMemoryQ();
  }

}

export function start<T>(logger: winston.LoggerInstance, argv: any): Queue<T> {
  return new Queue<T>(logger, argv);
}

export default Queue;
