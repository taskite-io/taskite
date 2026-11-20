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
package io.taskite.engine.util.record;

import io.taskite.protocol.impl.record.value.broker.*;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionDeletionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionRecord;
import io.taskite.protocol.record.Record;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.RecordValue;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.value.broker.TenantOwned;
import io.taskite.streamplatform.record.TypedRecordImpl;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

/** Compact, ordered test diagnostics using abbreviated names and original record keys. */
public final class RecordLogger {
  private static final int MAX_FIELD_LENGTH = 160;
  // Apply more specific names first; never abbreviate business IDs or field values.
  private static final List<Map.Entry<String, String>> ABBREVIATIONS =
      List.of(
          Map.entry("COMMAND_DISTRIBUTION", "CD"),
          Map.entry("PROCESS_INSTANCE", "PI"),
          Map.entry("DEPLOYMENT", "DPLY"),
          Map.entry("PROCESS", "PROC"),
          Map.entry("VARIABLE", "VAR"),
          Map.entry("RESOURCE", "RES"),
          Map.entry("ELEMENT_", ""),
          Map.entry("_ELEMENT", ""),
          Map.entry("EVENT", "EVNT"));
  private final List<? extends Record<?>> records;

  public RecordLogger(final Collection<? extends Record<?>> records) {
    this.records = List.copyOf(records);
  }

  /** Emits one block so concurrent tests do not interleave individual record lines. */
  public void log(final String testName) {
    System.out.print("Failed test: " + singleLine(testName) + "\n" + format());
  }

  public String format() {
    final var output =
        new StringBuilder("Engine execution log (" + records.size() + " records):\n");
    if (records.isEmpty()) {
      return output.append("  (no records)\n").toString();
    }
    for (final var record : records) {
      output
          .append("  #")
          .append(record.getPosition())
          .append(' ')
          .append(
              switch (record.getRecordType()) {
                case COMMAND -> "C";
                case EVENT -> "E";
                case COMMAND_REJECTION -> "R";
                default -> record.getRecordType().name();
              })
          .append(' ')
          .append(abbreviate(record.getValueType().name()))
          .append('.')
          .append(abbreviate(record.getIntent().name()))
          .append(" key=")
          .append(record.getKey())
          .append(" p=")
          .append(record.getPartitionId());
      if (record instanceof final TypedRecordImpl typed && typed.getSourceEventPosition() > 0) {
        output.append(" <-#").append(typed.getSourceEventPosition());
      }
      final var summary =
          record.getRecordType() == RecordType.COMMAND_REJECTION
              ? fields(
                  "rejection", record.getRejectionType(), "reason", record.getRejectionReason())
              : summarize(record.getValue());
      if (!summary.isEmpty()) {
        output.append(" | ").append(summary);
      }
      output.append('\n');
    }
    return output.toString();
  }

  private static String summarize(final RecordValue value) {
    final var summary =
        switch (value) {
          case null -> "";
          case final JobRecord job ->
              fields(
                  "type", job.getType(),
                  "retries", job.getRetries(),
                  "worker", job.getWorkerId(),
                  "lease", key(job.getWorkerLeaseToken()),
                  "deadline", key(job.getDeadline()),
                  "backoff", key(job.getRetryBackoff()),
                  "error", job.getErrorMessage(),
                  "variables", job.getVariables());
          case final JobBatchRecord batch ->
              fields(
                  "type", batch.getType(),
                  "worker", batch.getWorkerId(),
                  "lease", key(batch.getWorkerLeaseToken()),
                  "maxJobs", batch.getMaxJobsToActivate(),
                  "jobs", batch.getJobs().stream().map(JobBatchRecord.ActivatedJob::key).toList(),
                  "tenant", batch.getTenantId());
          case final IncidentRecord incident ->
              fields(
                  "job", key(incident.getJobKey()),
                  "errorType", incident.getErrorType(),
                  "error", incident.getErrorMessage());
          case final VariableRecord variable ->
              fields(
                  "scope", key(variable.getJobKey()),
                  "name", variable.getName(),
                  "value", variable.getValue());
          case final VariableDocumentRecord document ->
              fields(
                  "scope", key(document.getJobKey()),
                  "variables", document.getVariables());
          case final CommandDistributionRecord<?> distribution ->
              fields(
                      "distribution", key(distribution.getDistributionKey()),
                      "type", distribution.getValueType())
                  + " "
                  + summarize(distribution.getValue());
          case final JobSubscriptionRecord resource ->
              fields("resource", resource.getName(), "type", resource.getJobType());
          case final JobSubscriptionDeletionRecord resource ->
              fields("resource", key(resource.getSubscriptionKey()));
          default -> singleLine(value.toJson());
        };
    final var tenant =
        value instanceof final TenantOwned owned
                && !TenantOwned.DEFAULT_TENANT_IDENTIFIER.equals(owned.getTenantId())
            ? fields("tenant", owned.getTenantId())
            : "";
    return tenant.isEmpty() ? summary : summary + " " + tenant;
  }

  /** Alternating field names and values; omit unset keys and empty optional values. */
  private static String fields(final Object... fields) {
    final var summary = new StringJoiner(" ");
    for (int i = 0; i < fields.length; i += 2) {
      final var value = fields[i + 1];
      if (value == null
          || value instanceof final String text && text.isBlank()
          || value instanceof final Map<?, ?> map && map.isEmpty()
          || value instanceof final Collection<?> collection && collection.isEmpty()) {
        continue;
      }
      final var text =
          singleLine(
              value instanceof final String string
                  ? string.replace("\\", "\\\\").replace("\"", "\\\"")
                  : value instanceof final ValueType type
                      ? abbreviate(type.name())
                      : String.valueOf(value));
      summary.add(fields[i] + "=" + (value instanceof String ? "\"" + text + "\"" : text));
    }
    return summary.toString();
  }

  private static Long key(final long key) {
    return key > 0 ? key : null;
  }

  private static String abbreviate(final String name) {
    var abbreviation = name;
    for (final var replacement : ABBREVIATIONS) {
      abbreviation = abbreviation.replace(replacement.getKey(), replacement.getValue());
    }
    return abbreviation;
  }

  private static String singleLine(final String value) {
    final var escaped = value.replace("\r", "\\r").replace("\n", "\\n").replace("\t", "\\t");
    return escaped.length() > MAX_FIELD_LENGTH
        ? escaped.substring(0, MAX_FIELD_LENGTH) + "…"
        : escaped;
  }
}
