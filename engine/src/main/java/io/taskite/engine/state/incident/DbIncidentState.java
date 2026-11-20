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
package io.taskite.engine.state.incident;

import io.taskite.db.ColumnFamily;
import io.taskite.db.impl.DbForeignKey;
import io.taskite.db.impl.DbLong;
import io.taskite.engine.state.mutable.MutableIncidentState;
import io.taskite.protocol.ColumnFamilies;
import io.taskite.protocol.impl.record.value.broker.IncidentRecord;
import io.taskite.state.processing.ProcessingStateStore;
import java.util.Objects;

/** RocksDB-backed incidents indexed by job key. */
public final class DbIncidentState implements MutableIncidentState {

  // [incidentKey]
  private final DbLong incidentKey;
  private final IncidentRecordValue incidentValue;
  // INCIDENTS: [incidentKey] -> IncidentRecord MessagePack.
  private final ColumnFamily<DbLong, IncidentRecordValue> incidentsColumnFamilies;

  // [jobKey] -> JOBS
  private final DbForeignKey<DbLong> jobKey;
  // Incident key stored as the value of the element-instance or job index.
  private final DbLong indexedIncidentKey;
  // INCIDENT_JOBS: [jobKey FK -> JOBS] -> incidentKey.
  private final ColumnFamily<DbForeignKey<DbLong>, DbLong> jobIncidentsColumnFamilies;

  public DbIncidentState(final ProcessingStateStore storage) {
    incidentKey = new DbLong();
    incidentValue = new IncidentRecordValue();
    jobKey = new DbForeignKey<>(new DbLong(), ColumnFamilies.JOBS);
    indexedIncidentKey = new DbLong();

    Objects.requireNonNull(storage, "storage");
    incidentsColumnFamilies =
        storage.createColumnFamily(ColumnFamilies.INCIDENTS, incidentKey, incidentValue);
    jobIncidentsColumnFamilies =
        storage.createColumnFamily(ColumnFamilies.INCIDENT_JOBS, jobKey, new DbLong());
  }

  @Override
  public IncidentRecord getIncident(final long key) {
    incidentKey.wrapLong(key);
    final var stored = incidentsColumnFamilies.get(incidentKey, IncidentRecordValue::new);
    return stored == null ? null : stored.getRecord();
  }

  @Override
  public long getJobIncidentKey(final long jobKey) {
    this.jobKey.inner().wrapLong(jobKey);
    final DbLong stored = jobIncidentsColumnFamilies.get(this.jobKey);
    return stored == null ? MISSING_INCIDENT : stored.getValue();
  }

  @Override
  public long getIncidentCount() {
    return incidentsColumnFamilies.count();
  }

  @Override
  public void createIncident(final long key, final IncidentRecord incident) {
    incidentKey.wrapLong(key);
    incidentValue.setRecord(incident);
    incidentsColumnFamilies.insert(incidentKey, incidentValue);

    indexedIncidentKey.wrapLong(key);
    jobKey.inner().wrapLong(incident.getJobKey());
    jobIncidentsColumnFamilies.insert(jobKey, indexedIncidentKey);
  }

  @Override
  public void deleteIncident(final long key) {
    final IncidentRecord incident = getIncident(key);
    if (incident == null) {
      return;
    }

    incidentKey.wrapLong(key);
    incidentsColumnFamilies.deleteExisting(incidentKey);
    jobKey.inner().wrapLong(incident.getJobKey());
    jobIncidentsColumnFamilies.deleteExisting(jobKey);
  }
}
