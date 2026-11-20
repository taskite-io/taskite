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
package io.taskite.engine.util.client;

import io.taskite.protocol.impl.record.value.broker.IncidentRecord;
import io.taskite.protocol.record.Record;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.IncidentIntent;
import java.util.Objects;

public final class IncidentClient {
  private final CommandWriter writer;
  private final IncidentRecord value = new IncidentRecord();
  private long key = -1;

  public IncidentClient(final CommandWriter writer) {
    this.writer = Objects.requireNonNull(writer, "writer");
  }

  public IncidentClient withKey(final long incidentKey) {
    key = incidentKey;
    return this;
  }

  public IncidentClient withTenantId(final String tenantId) {
    value.setTenantId(tenantId);
    return this;
  }

  private long resolveKey() {
    return key;
  }

  public Record<IncidentRecord> resolve(final long incidentKey) {
    return withKey(incidentKey).resolve();
  }

  public Record<IncidentRecord> resolve() {
    final long incidentKey = resolveKey();
    return writer.send(
        incidentKey,
        ValueType.INCIDENT,
        IncidentIntent.RESOLVE,
        value,
        IncidentIntent.RESOLVED,
        incidentKey,
        IncidentRecord.class);
  }

  /** Queries by element-instance or job key and returns a record keyed by the incident. */
  public Record<IncidentRecord> query(final long ownerKey) {
    return writer.send(
        ownerKey,
        ValueType.INCIDENT,
        IncidentIntent.QUERY,
        value,
        IncidentIntent.QUERIED,
        null,
        IncidentRecord.class);
  }

  public RejectionClient expectRejection() {
    return new RejectionClient();
  }

  public final class RejectionClient {
    private RejectionClient() {}

    public Record<?> resolve(final long incidentKey) {
      withKey(incidentKey);
      return resolve();
    }

    public Record<?> resolve() {
      final long incidentKey = resolveKey();
      return writer.reject(
          incidentKey,
          ValueType.INCIDENT,
          IncidentIntent.RESOLVE,
          value,
          IncidentIntent.RESOLVED,
          incidentKey);
    }
  }
}
