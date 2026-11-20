/*
 * Copyright (c) 2026 lzgabel and Taskite contributors
 *
 * Licensed under the Taskite Community License, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *     https://github.com/taskite-io/taskite/blob/main/LICENSE
 *
 * The Software is source-available for non-commercial use and limited evaluation
 * use. Other commercial use requires a separate written license. The Software is
 * provided "AS IS", without warranties or conditions of any kind.
 */
package io.taskite.protocol.record.value.broker;

import io.taskite.protocol.record.RecordValueWithVariables;
import io.taskite.protocol.record.intent.broker.JobIntent;

/**
 * Represents a job related event or command.
 *
 * <p>See {@link JobIntent} for intents.
 */
public interface JobRecordValue extends RecordValueWithVariables, TenantOwned {

  /**
   * @return the type of the job
   */
  String getType();

  /**
   * @return user-defined headers associated with this job
   */

  /**
   * @return the assigned worker to complete the job
   */
  String getWorker();

  /** PD-registered Worker instance, distinct from the display name. */
  String getWorkerId();

  /** Stable PD registration token; zero denotes a legacy job without session ownership. */
  long getWorkerLeaseToken();

  /**
   * @return remaining retries
   */
  int getRetries();

  /** Job activation priority; higher values are selected first. Defaults to zero. */
  int getPriority();

  /**
   * @return the time of backoff in milliseconds. If backoff is disabled this method returns 0
   *     (default value).
   */
  long getRetryBackoff();

  /**
   * @return the timestamp when this job record should be recurred. This method is used by backoff
   *     to determine the date when the job is needed to be recurred after backoff.
   */
  long getRecurringTime();

  /**
   * @return the unix timestamp until when the job is exclusively assigned to this worker (time unit
   *     is milliseconds since unix epoch). If the deadline is exceeded, it can happen that the job
   *     is handed to another worker and the work is performed twice. If this property is not set it
   *     will return '-1'.
   */
  long getDeadline();

  /**
   * @return the duration used to update the deadline. If this property is not set it will return
   *     '-1'.
   */
  long getTimeout();

  /**
   * @return the message that contains additional context of the failure/error. It is set by the job
   *     worker then the processing fails because of a technical failure or a non-technical error.
   */
  String getErrorMessage();

  /**
   * @return the error code to identify the business error. It is set by the job worker then the
   *     processing fails because of a non-technical error.
   */
  String getErrorCode();

  long getSubscriptionKey();
}
