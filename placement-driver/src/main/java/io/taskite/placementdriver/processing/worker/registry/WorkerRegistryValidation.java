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
package io.taskite.placementdriver.processing.worker.registry;

import io.taskite.protocol.impl.record.value.placementdriver.worker.WorkerRegistrationRecord;
import java.util.List;

final class WorkerRegistryValidation {
  private static final char KEY_SEPARATOR = '\u001f';
  private static final String MISSING_TENANTS =
      "Expected worker tenantIds to contain at least one tenant, but the list is empty";
  private static final String MISSING_JOB_TYPE =
      "Expected worker jobType to be present, but it is blank";
  private static final String INVALID_MAX_JOBS =
      "Expected worker maxJobsActive to be positive, but was '%d'";
  private static final String INVALID_ACTIVE_JOBS =
      "Expected worker activeJobs to be between zero and maxJobsActive '%d', but was '%d'";
  private static final String INVALID_CONCURRENCY =
      "Expected worker concurrency to be positive, but was '%d'";
  private static final String INVALID_TIMEOUT =
      "Expected worker timeoutMillis to be nonnegative, but was '%d'";
  private static final String INVALID_POLL_INTERVAL =
      "Expected worker pollIntervalMillis to be nonnegative, but was '%d'";
  private static final String INVALID_RENEWAL_TIMEOUT =
      "Expected worker renewalTimeoutMillis to be positive, but was '%d'";
  private static final String MISSING_IDENTIFIER =
      "Expected worker field '%s' to be present, but was '%s'";
  private static final String IDENTIFIER_WHITESPACE =
      "Expected worker field '%s' to have no leading or trailing whitespace, but was '%s'";
  private static final String IDENTIFIER_TOO_LONG =
      "Expected worker field '%s' to contain at most 256 characters, but had '%d'";
  private static final String IDENTIFIER_CONTROL_CHARACTER =
      "Expected worker field '%s' to contain no registry key separator, but an unsupported control character was found";

  private WorkerRegistryValidation() {}

  static String validateIdentity(final WorkerRegistrationRecord worker) {
    final String clusterError = identifierError("clusterId", worker.getClusterId());
    if (clusterError != null) {
      return clusterError;
    }
    final String workerIdError = identifierError("workerId", worker.getWorkerId());
    if (workerIdError != null) {
      return workerIdError;
    }
    final String nameError = identifierError("name", worker.getName());
    if (nameError != null) {
      return nameError;
    }
    final List<String> tenantIds = worker.getTenantIds();
    if (tenantIds.isEmpty()) {
      return MISSING_TENANTS;
    }
    for (final String tenantId : tenantIds) {
      final String tenantError = identifierError("tenantId", tenantId);
      if (tenantError != null) {
        return tenantError;
      }
    }
    return null;
  }

  static String validateRegistration(final WorkerRegistrationRecord worker) {
    final String identityError = validateIdentity(worker);
    if (identityError != null) {
      return identityError;
    }
    if (worker.getJobType().isBlank()) {
      return MISSING_JOB_TYPE;
    }
    if (worker.getMaxJobsActive() <= 0) {
      return INVALID_MAX_JOBS.formatted(worker.getMaxJobsActive());
    }
    if (worker.getActiveJobs() < 0 || worker.getActiveJobs() > worker.getMaxJobsActive()) {
      return INVALID_ACTIVE_JOBS.formatted(worker.getMaxJobsActive(), worker.getActiveJobs());
    }
    if (worker.getConcurrency() <= 0) {
      return INVALID_CONCURRENCY.formatted(worker.getConcurrency());
    }
    if (worker.getTimeoutMillis() < 0) {
      return INVALID_TIMEOUT.formatted(worker.getTimeoutMillis());
    }
    if (worker.getPollIntervalMillis() < 0) {
      return INVALID_POLL_INTERVAL.formatted(worker.getPollIntervalMillis());
    }
    if (worker.getRenewalTimeoutMillis() <= 0) {
      return INVALID_RENEWAL_TIMEOUT.formatted(worker.getRenewalTimeoutMillis());
    }
    return null;
  }

  private static String identifierError(final String field, final String value) {
    if (value == null || value.isBlank()) {
      return MISSING_IDENTIFIER.formatted(field, value);
    }
    if (!value.equals(value.trim())) {
      return IDENTIFIER_WHITESPACE.formatted(field, value);
    }
    if (value.length() > 256) {
      return IDENTIFIER_TOO_LONG.formatted(field, value.length());
    }
    if (value.indexOf(KEY_SEPARATOR) >= 0) {
      return IDENTIFIER_CONTROL_CHARACTER.formatted(field);
    }
    return null;
  }
}
