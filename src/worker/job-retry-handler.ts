/**
 * Background worker job error retry and dead-letter handling (#881).
 *
 * Provides a standardized execution loop for worker tasks, incorporating:
 * - Retry counter tracking and limit enforcement
 * - Exponential backoff with jitter
 * - Transient error recovery vs permanent failure fast-abort
 * - Dead-letter logging and database status transition to "failed"
 */
import { createLogger } from "../lib/logger";
import { jobLogger } from "../lib/worker-logger";
import { audit } from "../services/audit";
import { AuditAction } from "../services/audit-actions";
import {
  asJobError,
  retryDelayMs,
  type JobFailureCategory,
  type RetryPolicy,
} from "../services/job-retry";
import { recordTaskDeadLetter, recordTaskFailure } from "./health";

const log = createLogger({ name: "worker:retry-handler" });

export interface WorkerJobExecutionOptions {
  taskName: string;
  /** Job family used in structured logs; defaults to `worker_task`. */
  jobType?: string;
  jobId?: string;
  userId?: string | null;
  groupId?: string | null;
  policy?: RetryPolicy;
  sleep?: (ms: number) => Promise<void>;
  auditDeadLetter?: boolean;
  /**
   * Optional recovery hook for indeterminate failures (e.g. timeouts or dropped
   * sockets where a Stellar transaction may have already applied on the ledger).
   * If this hook resolves to `true`, the job is treated as already concluded and
   * no further retry or dead-letter transition is performed.
   */
  onIndeterminate?: (info: {
    attempt: number;
    reason: string;
    error: unknown;
  }) => Promise<boolean>;
  onRetry?: (info: {
    attempt: number;
    delayMs: number;
    category: JobFailureCategory;
    reason: string;
    error: unknown;
  }) => void | Promise<void>;
  onDeadLetter?: (info: {
    attempts: number;
    category: JobFailureCategory;
    reason: string;
    error: unknown;
  }) => Promise<void>;
}

export interface WorkerJobExecutionResult<T> {
  success: boolean;
  result?: T;
  error?: unknown;
  attempts: number;
  deadLettered: boolean;
  lastCategory?: JobFailureCategory;
  lastReason?: string;
}

export const DEFAULT_WORKER_JOB_POLICY: RetryPolicy = {
  maxAttempts: 3,
  initialDelayMs: 500,
  maxDelayMs: 10_000,
  jitterRatio: 0.1,
};

const defaultSleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

export async function executeWorkerJobWithRetry<T>(
  task: (attempt: number) => Promise<T>,
  options: WorkerJobExecutionOptions
): Promise<WorkerJobExecutionResult<T>> {
  const policy = options.policy ?? DEFAULT_WORKER_JOB_POLICY;
  const sleep = options.sleep ?? defaultSleep;
  const taskName = options.taskName;
  const jobType = options.jobType ?? "worker_task";
  const jobId = options.jobId ?? taskName;
  const job = jobLogger(jobType, jobId, log);

  let lastError: unknown;

  job.log("started", {
    task: taskName,
    attempt: 1,
    maxAttempts: policy.maxAttempts,
  });

  for (let attempt = 1; attempt <= policy.maxAttempts; attempt += 1) {
    try {
      const result = await task(attempt);
      job.log("completed", {
        task: taskName,
        attempt,
        maxAttempts: policy.maxAttempts,
      });
      return {
        success: true,
        result,
        attempts: attempt,
        deadLettered: false,
      };
    } catch (error) {
      lastError = error;
      const classified = asJobError(error);
      let { category, retryable } = classified;
      const { message: reason } = classified;

      if (category === "indeterminate" && options.onIndeterminate) {
        const concluded = await options.onIndeterminate({
          attempt,
          reason,
          error,
        });
        if (concluded) {
          job.log("completed", {
            task: taskName,
            attempt,
            maxAttempts: policy.maxAttempts,
            outcome: "already_applied",
          });
          return {
            success: true,
            attempts: attempt,
            deadLettered: false,
            lastCategory: category,
            lastReason: reason,
          };
        }
        category = "transient";
        retryable = true;
      }

      recordTaskFailure(taskName);

      if (!retryable || attempt >= policy.maxAttempts) {
        recordTaskDeadLetter(taskName);
        job.error(error, {
          task: taskName,
          attempt,
          attempts: attempt,
          maxAttempts: policy.maxAttempts,
          category,
          retryable,
          reason,
          outcome: "dead_letter",
        });

        if (options.auditDeadLetter) {
          await audit({
            userId: options.userId ?? null,
            groupId: options.groupId ?? null,
            actorType: "worker",
            action: AuditAction.WORKER_TASK_DEAD_LETTER,
            entityType: jobType,
            entityId: jobId,
            outcome: "failure",
            metadata: {
              task: taskName,
              attempts: attempt,
              maxAttempts: policy.maxAttempts,
              category,
              reason,
            },
          }).catch(() => undefined);
        }

        if (options.onDeadLetter) {
          await options.onDeadLetter({
            attempts: attempt,
            category,
            reason,
            error,
          });
        }

        return {
          success: false,
          error,
          attempts: attempt,
          deadLettered: true,
          lastCategory: category,
          lastReason: reason,
        };
      }

      const delayMs = retryDelayMs(attempt, policy);
      job.warn("retry_scheduled", {
        task: taskName,
        attempt,
        maxAttempts: policy.maxAttempts,
        category,
        retryable,
        delayMs,
        reason,
        err: error,
      });

      if (options.onRetry) {
        await options.onRetry({
          attempt,
          delayMs,
          category,
          reason,
          error,
        });
      }

      await sleep(delayMs);
    }
  }

  return {
    success: false,
    error: lastError,
    attempts: policy.maxAttempts,
    deadLettered: true,
  };
}
