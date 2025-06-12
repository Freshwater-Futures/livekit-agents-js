// SPDX-FileCopyrightText: 2024 LiveKit, Inc.
//
// SPDX-License-Identifier: Apache-2.0
import { MultiMutex, Mutex } from '@livekit/mutex';
import type { RunningJobInfo } from '../job.js';
import { log } from '../log.js';
import { Queue } from '../utils.js';
import type { InferenceExecutor } from './inference_executor.js';
import type { JobExecutor } from './job_executor.js';
import { JobProcExecutor } from './job_proc_executor.js';

export class ProcPool {
  agent: string;
  initializeTimeout: number;
  closeTimeout: number;
  executors: JobExecutor[] = [];
  tasks: Promise<void>[] = [];
  started = false;
  closed = false;
  controller = new AbortController();
  initMutex = new Mutex();
  procMutex?: MultiMutex;
  procUnlock?: () => void;
  warmedProcQueue = new Queue<JobExecutor>();
  inferenceExecutor?: InferenceExecutor;
  memoryWarnMB: number;
  memoryLimitMB: number;
  #numIdleProcesses: number;
  #logger = log();

  constructor(
    agent: string,
    numIdleProcesses: number,
    initializeTimeout: number,
    closeTimeout: number,
    inferenceExecutor: InferenceExecutor | undefined,
    memoryWarnMB: number,
    memoryLimitMB: number,
  ) {
    this.agent = agent;
    this.#numIdleProcesses = numIdleProcesses;
    if (numIdleProcesses > 0) {
      this.procMutex = new MultiMutex(numIdleProcesses);
    }
    this.initializeTimeout = initializeTimeout;
    this.closeTimeout = closeTimeout;
    this.inferenceExecutor = inferenceExecutor;
    this.memoryWarnMB = memoryWarnMB;
    this.memoryLimitMB = memoryLimitMB;
  }

  get processes(): JobExecutor[] {
    return this.executors;
  }

  getByJobId(id: string): JobExecutor | null {
    return this.executors.find((x) => x.runningJob && x.runningJob.job.id === id) || null;
  }

  async launchJob(info: RunningJobInfo) {
    let proc: JobExecutor;
    if (this.procMutex) {
      this.#logger.debug('Launching job. Acquiring an idle process from queue for job', {
        jobId: info.job.id,
      });
      proc = await this.warmedProcQueue.get();
      if (this.procUnlock) {
        this.procUnlock();
        this.procUnlock = undefined;
      }
    } else {
      proc = new JobProcExecutor(
        this.agent,
        this.inferenceExecutor,
        this.initializeTimeout,
        this.closeTimeout,
        this.memoryWarnMB,
        this.memoryLimitMB,
        2500,
        60000,
        500,
      );
      this.executors.push(proc);
      await proc.start();
      await proc.initialize();
    }
    await proc.launchJob(info);
  }

  async procWatchTask() {
    this.#logger.debug(`Starting process watcher task (target idle: ${this.#numIdleProcesses})`);
    const proc = new JobProcExecutor(
      this.agent,
      this.inferenceExecutor,
      this.initializeTimeout,
      this.closeTimeout,
      this.memoryWarnMB,
      this.memoryLimitMB,
      2500,
      60000,
      500,
    );

    try {
      this.executors.push(proc);
      this.#logger.info(`Number of processes in pool: ${this.executors.length}`);
      const unlock = await this.initMutex.lock();
      if (this.closed) {
        return;
      }

      await proc.start();
      this.#logger.info(`Started child process PID: ${proc.proc?.pid}`);
      try {
        await proc.initialize();
        this.#logger.info(`Initialized child process PID: ${proc.proc?.pid}`);
        await this.warmedProcQueue.put(proc);
        this.#logger.info(
          `Added idle process PID: ${proc.proc?.pid} to queue (current idle: ${this.warmedProcQueue.items.length})`,
        );
      } catch (err) {
        this.#logger.error({ err, pid: proc.proc?.pid }, 'Failed to initialize child process');
        if (this.procUnlock) {
          this.procUnlock();
          this.procUnlock = undefined;
        }
      }

      unlock();
      await proc.join();
    } finally {
      this.#logger.warn(
        `Child process PID: ${proc.proc?.pid} exited. Removing from pool. (current idle: ${this.warmedProcQueue.items.length})`,
      );
      this.executors.splice(this.executors.indexOf(proc));
    }
  }

  start() {
    if (this.started) {
      return;
    }

    this.started = true;
    this.run(this.controller.signal);
  }

  async run(signal: AbortSignal) {
    if (this.procMutex) {
      while (!signal.aborted) {
        this.procUnlock = await this.procMutex.lock();
        const task = this.procWatchTask();
        this.tasks.push(task);
        task.finally(() => this.tasks.splice(this.tasks.indexOf(task)));
      }
    }
  }

  async close() {
    if (!this.started) {
      return;
    }
    this.closed = true;
    this.controller.abort();
    const activeJobs = this.executors.filter((e) => e !== undefined && e.runningJob).length;
    this.#logger.info(
      `Closing ProcPool. Shutting down ${this.warmedProcQueue.items.length} idle processes and ${activeJobs} active job processes.`,
    );
    this.warmedProcQueue.items.forEach((e) => e.close());
    this.executors.forEach((e) => e.close());
    await Promise.allSettled(this.tasks);
  }
}
