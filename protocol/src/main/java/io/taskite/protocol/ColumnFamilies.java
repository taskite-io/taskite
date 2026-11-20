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
package io.taskite.protocol;

/**
 * The complete set of logical column families in a partition's processing state. They share one
 * physical RocksDB column family and are distinguished by an explicit stable id written as a key
 * prefix (see {@code ColumnFamilyContext}).
 *
 * <p>IDs 1-199 are reserved for processing, stream and consensus metadata. JRaft currently owns its
 * raft-log and raft-meta stores outside this logical database. IDs 200-499 are reserved for
 * placement, membership and topology state. IDs 500-999 are reserved for engine runtime state.
 */
public enum ColumnFamilies implements EnumValue {

  ////////////////////////////////////////////////////////////////////////////////////////////////
  ///////////////////// 1-199: processing, stream and consensus metadata  ////////////////////////
  ////////////////////////////////////////////////////////////////////////////////////////////////

  /** Processing metadata: last processed position. */
  PROCESSING_META(1),
  /** Generated-key high-water mark (see {@code PersistedKeyGenerator}). */
  KEY(2),

  ////////////////////////////////////////////////////////////////////////////////////////////////
  /////// 200-499: placement-driver cluster, membership, partition and topology state  ///////////
  ////////////////////////////////////////////////////////////////////////////////////////////////

  // Placement-driver cluster ids.
  PD_CLUSTER_IDS(200),
  // Placement-driver broker registrations, keyed by clusterId:brokerId.
  PD_BROKER_REGISTRATIONS(201),
  // Placement-driver topology configurations, keyed by cluster id.
  PD_TOPOLOGY_CONFIGS(202),
  // Placement-driver member topology, keyed by a singleton id.
  PD_MEMBER_TOPOLOGY(203),
  /**
   * Placement-driver persisted partition topology (placement source of truth), keyed by
   * clusterId:partitionId.
   */
  PD_PARTITION_TOPOLOGY(204),

  // Placement-driver command distribution records, keyed by distribution key.
  PD_COMMAND_DISTRIBUTIONS(205),
  // Placement-driver pending command distributions, keyed by distribution key and partition id.
  PD_PENDING_DISTRIBUTIONS(206),
  // Placement-driver command distribution dedupe index, keyed by business uniqueness key.
  PD_COMMAND_DISTRIBUTION_DEDUPE(207),

  // Placement-driver partition-change plans, keyed by clusterId:changeId.
  PD_CHANGE_PLANS(208),
  // Placement-driver pending partition-change plans, keyed by clusterId:partitionId.
  PD_PENDING_CHANGES(209),

  /**
   * Placement-driver partition-change operation dedupe index, keyed by operation idempotency key.
   */
  PD_TOPOLOGY_OP_DEDUPE(210),
  // Placement-driver aggregate cluster-change manifests, keyed by clusterId:changeId.
  PD_CLUSTER_CHANGE_OPERATIONS(211),
  // Placement-driver active aggregate change id, keyed by clusterId.
  PD_ACTIVE_CLUSTER_CHANGES(212),

  // Broker/PD member-change manifests, keyed by scope:resourceId:changeId.
  PD_MEMBER_CHANGES(213),
  // Active Broker/PD member-change id, keyed by scope:resourceId.
  PD_ACTIVE_MEMBER_CHANGES(214),
  // Latest target member configuration, keyed by scope:resourceId.
  PD_MEMBER_CONFIGURATIONS(215),
  // Broker durable topology-operation manifest, keyed by operation id and latest pointer.
  BROKER_TOPOLOGY_OPERATIONS(216),
  /**
   * Broker durable last-applied fenced-recovery epoch per partition (Q6 monotonic guard), keyed by
   * a singleton id within the partition's own storage.
   */
  BROKER_RECOVERY_EPOCHS(217),
  /**
   * PD current Worker registrations, keyed by [cluster, tenant, Worker instance id]. Stores
   * lifecycle, consumption configuration, REGISTER request id, token and lease for current-session
   * retry idempotency; removed when offline retention expires.
   */
  PD_WORKER_REGISTRATIONS(218),
  // Placement-driver Broker/Worker deadlines, keyed by deadline, type and resource identity.
  PD_REGISTRATION_DEADLINES(219),
  // Placement-driver job subscription mirror records, keyed by mirror key.
  PD_JOB_SUBSCRIPTION_MIRRORS(220),
  // Placement-driver job subscription mirror pair index, keyed by sourceClusterId:targetClusterId.
  PD_JOB_SUBSCRIPTION_MIRROR_PAIRS(221),
  // Placement-driver command-distribution targets, keyed by distributionKey:targetClusterId.
  PD_COMMAND_DISTRIBUTION_TARGETS(222),
  // Explicitly removed Brokers, excluded from placement until an explicit member re-add.
  PD_BROKER_PLACEMENT_EXCLUSIONS(223),
  /**
   * PD task-release plans, keyed by [cluster id, yield id]. Stores the exited Worker session
   * snapshot and per-partition ACK, retry and released-job progress. Supports bounded cluster
   * history synchronization directly; retained until Worker retention cleanup.
   */
  PD_JOB_YIELDS(224),
  /**
   * PD pending task-release index, keyed by yield id with the cluster id as value. Locates the
   * complete plan and drives dispatch and retries; completion removes this entry while retaining
   * the plan in PD_JOB_YIELDS.
   */
  PD_PENDING_JOB_YIELDS(225),
  // PD job schedule timers, keyed by timer key.
  PD_TIMERS(226),
  // PD timer due-date index, keyed by [dueDate, timerKey].
  PD_TIMER_DUE_DATES(227),
  // Active task templates, keyed by subscription distribution key.
  PD_JOB_SUBSCRIPTIONS(228),
  /** Rebuildable structured indexes for bounded leader workflow checks. */
  PD_PENDING_DISTRIBUTION_TARGETS(231),
  PD_PENDING_PARTITION_CHANGES(232),
  PD_PARTITION_TOPOLOGY_INDEX(233),
  /**
   * B distribution attempts: [distribution key, target cluster, partition] ->
   * generation/attempt/due.
   */
  PD_BROKER_DISTRIBUTION_SCHEDULES(234),
  /** B distribution deadlines: [due, distribution key, target cluster, partition] -> schedule. */
  PD_BROKER_DISTRIBUTION_DUE_DATES(235),
  /** [kind, dueAt, yield FK pending, partitionId] -> clusterId; due/reservation namespaces. */
  PD_JOB_YIELD_DUE_DATES(236),
  /** [clusterId, partitionId] -> sync cursor, request identity and deadline. */
  PD_JOB_YIELD_SYNCHRONIZATION(237),
  /** [kind, dueAt, sync FK] -> nil; due/reservation namespaces, FK to synchronization. */
  PD_JOB_YIELD_SYNC_DUE_DATES(238),
  /** [cluster, partition] -> dirty revision, durable expansion/recovery cursor and observation. */
  PD_BROKER_TOPOLOGY_SCHEDULES(239),
  /** [dueAt, FK -> topology schedules] -> nil. */
  PD_BROKER_TOPOLOGY_DUE_DATES(240),
  /** [scope, cluster, change, partition, operation] -> independent task state and history. */
  PD_TOPOLOGY_CHANGE_TASKS(241),
  /** [kind, deadline, FK -> tasks] -> nil; observation and RPC reservation indexes. */
  PD_TOPOLOGY_CHANGE_TASK_DUE_DATES(242),
  /** [scope, cluster, partition] -> FK -> current nonterminal task. */
  PD_ACTIVE_TOPOLOGY_CHANGE_TASKS(243),
  /** [revisionSequence | queueTime] -> long; global topology coordinator metadata. */
  PD_BROKER_TOPOLOGY_COORDINATOR_META(244),

  ////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////// 500-999: engine runtime state  ////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////////////////////////

  // Broker job subscription templates, keyed by PD distribution key.
  BROKER_JOB_SUBSCRIPTIONS(500),
  // Subscription lookup index, keyed by [jobType, tenantId, subscriptionKey] -> nil.
  BROKER_JOB_SUBSCRIPTIONS_BY_TYPE(502),
  // Variables, keyed by [job key, variable name].
  VARIABLES(522),

  // Job state, keyed by job key.
  JOBS(540),
  // Per-job lifecycle state, keyed by a foreign key into {@link #JOBS}.
  JOB_STATES(541),
  // Index of activatable jobs, keyed by [type, invertedPriority, jobKey, tenant].
  JOB_ACTIVATABLE(542),
  // Activated jobs ordered by [deadline, jobKey].
  JOB_DEADLINES(543),
  // Retriable failed jobs ordered by [recurringTime, jobKey].
  JOB_BACKOFF(544),
  /**
   * Broker ownership index of activated jobs, keyed by [tenant, Worker id, lease token, job key]
   * with an empty value. Finds jobs to release for one exited session; entries are removed when
   * jobs leave ACTIVATED.
   */
  JOB_ACTIVATED_BY_WORKER(545),
  /**
   * Broker task-release records and session fences: operation:{yieldId} stores progress,
   * session-prefixed keys identify revoked [tenant, Worker id, lease token] sessions, and
   * sync:cursor tracks PD history synchronization. Session fences survive release completion and
   * recovery.
   */
  JOB_YIELDS(546),
  /**
   * Broker ownership-index backfill metadata: key 0 stores the last scanned job key and key 1 marks
   * completion. Makes JOB_ACTIVATED_BY_WORKER migration resumable after recovery.
   */
  JOB_INDEX_METADATA(547),
  /**
   * Broker pending task-release index, keyed by yield id with an empty value. Drives bounded
   * release batches and is removed on completion, while session fences remain in JOB_YIELDS.
   */
  PENDING_JOB_YIELDS(548),
  // In-flight jobs belonging to a subscription, keyed by [subscriptionKey, jobKey] -> nil.
  JOBS_BY_SUBSCRIPTION(549),

  // Engine incidents, keyed by incident key.
  INCIDENTS(560),
  // Job incident index, keyed by job key.
  INCIDENT_JOBS(562);

  private final int value;

  ColumnFamilies(final int value) {
    this.value = value;
  }

  @Override
  public int getValue() {
    return value;
  }
}
