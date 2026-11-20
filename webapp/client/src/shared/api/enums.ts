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
export enum ChangeAcceptanceOutcome {
  NO_CHANGES = 'NO_CHANGES',
}

export enum ChangeStatus {
  QUEUED = 'QUEUED',
  PENDING = 'PENDING',
  WAITING_CATCH_UP = 'WAITING_CATCH_UP',
  IN_PROGRESS = 'IN_PROGRESS',
  WAITING_READY = 'WAITING_READY',
  WAITING_LEADER_CONVERGE = 'WAITING_LEADER_CONVERGE',
  SUPERSEDING = 'SUPERSEDING',
  SUPERSEDED = 'SUPERSEDED',
  CANCELLING = 'CANCELLING',
  COMPLETED = 'COMPLETED',
  FAILED = 'FAILED',
  CANCELLED = 'CANCELLED',
}

export enum WorkerStatus {
  ONLINE = 'ONLINE',
  OFFLINE = 'OFFLINE',
}

export enum JobYieldStatus {
  PENDING = 'PENDING',
  COMPLETED = 'COMPLETED',
  RETIRED = 'RETIRED',
}

export enum JobYieldReason {
  WORKER_LEASE_EXPIRED = 'WORKER_LEASE_EXPIRED',
  WORKER_DEREGISTERED = 'WORKER_DEREGISTERED',
  WORKER_SESSION_REPLACED = 'WORKER_SESSION_REPLACED',
}

export enum RegistrationStatus {
  UNKNOWN = 'UNKNOWN',
  ONLINE = 'ONLINE',
  OFFLINE = 'OFFLINE',
  DRAINING = 'DRAINING',
}

export enum MemberRole {
  UNKNOWN = 'UNKNOWN',
  REGISTERED = 'REGISTERED',
  VOTER = 'VOTER',
  LEADER = 'LEADER',
  CANDIDATE = 'CANDIDATE',
  FOLLOWER = 'FOLLOWER',
  LEARNER = 'LEARNER',
  INACTIVE = 'INACTIVE',
}

export enum PartitionLifecycle {
  ABSENT = 'ABSENT',
  JOINING = 'JOINING',
  CATCHING_UP = 'CATCHING_UP',
  ACTIVE = 'ACTIVE',
  LEAVING = 'LEAVING',
  FAULTED = 'FAULTED',
}

export enum PartitionStatus {
  CREATING = 'CREATING',
  ACTIVE = 'ACTIVE',
  REBALANCING = 'REBALANCING',
  DELETING = 'DELETING',
}

export enum PartitionOperationStatus {
  NONE = 'NONE',
  IN_PROGRESS = 'IN_PROGRESS',
  APPLIED = 'APPLIED',
  FAILED = 'FAILED',
}

export enum RaftNodeState {
  UNKNOWN = 'UNKNOWN',
  BOOTSTRAP = 'BOOTSTRAP',
  RECOVERED = 'RECOVERED',
  SNAPSHOT = 'SNAPSHOT',
  JOINT_SNAPSHOT_BASELINE = 'JOINT_SNAPSHOT_BASELINE',
  STATE_LEADER = 'STATE_LEADER',
  STATE_TRANSFERRING = 'STATE_TRANSFERRING',
  STATE_CANDIDATE = 'STATE_CANDIDATE',
  STATE_FOLLOWER = 'STATE_FOLLOWER',
  STATE_ERROR = 'STATE_ERROR',
  STATE_UNINITIALIZED = 'STATE_UNINITIALIZED',
  STATE_SHUTTING = 'STATE_SHUTTING',
  STATE_SHUTDOWN = 'STATE_SHUTDOWN',
  STATE_END = 'STATE_END',
}

export enum DistributionStatus {
  DISTRIBUTING = 'DISTRIBUTING',
  COMPLETED = 'COMPLETED',
  DELETING = 'DELETING',
  DELETED = 'DELETED',
}

export enum JobSubscriptionCreationType {
  SUBSCRIPTION = 'SUBSCRIPTION',
  MIRROR = 'MIRROR',
}

export enum MirrorStatus {
  ACTIVE = 'ACTIVE',
}

export enum HealthStatus {
  UP = 'UP',
}

export enum ChangeType {
  NONE = '',
  CLUSTER = 'CLUSTER',
  MEMBER = 'MEMBER',
  PARTITION = 'PARTITION',
}

export enum MemberOperationType {
  MEMBER_JOIN = 'MEMBER_JOIN',
  MEMBER_ADD_LEARNER = 'MEMBER_ADD_LEARNER',
  MEMBER_PROMOTE_VOTER = 'MEMBER_PROMOTE_VOTER',
  TRANSFER_LEADER = 'TRANSFER_LEADER',
  MEMBER_LEAVE = 'MEMBER_LEAVE',
  MEMBER_REMOVE = 'MEMBER_REMOVE',
}

export enum TopologyOperationType {
  ADD = 'ADD',
  JOIN = 'JOIN',
  PROMOTE_LEARNER = 'PROMOTE_LEARNER',
  LEAVE = 'LEAVE',
  RECONFIGURE_PRIORITY = 'RECONFIGURE_PRIORITY',
  TRANSFER_LEADER = 'TRANSFER_LEADER',
  STEP_DOWN_IF_NOT_PRIMARY = 'STEP_DOWN_IF_NOT_PRIMARY',
  RESET_PEERS = 'RESET_PEERS',
}

export enum PlanBlockedReason {
  NONE = '',
  CONFIGURATION_CONFLICT = 'CONFIGURATION_CONFLICT',
  TARGET_UNAVAILABLE = 'TARGET_UNAVAILABLE',
  PLAN_DEADLINE_EXCEEDED = 'PLAN_DEADLINE_EXCEEDED',
  JOB_SUBSCRIPTION_DISTRIBUTION_TIMEOUT = 'JOB_SUBSCRIPTION_DISTRIBUTION_TIMEOUT',
  NO_QUORUM = 'NO_QUORUM',
  INSUFFICIENT_CAPACITY = 'INSUFFICIENT_CAPACITY',
  LEADER_UNAVAILABLE = 'LEADER_UNAVAILABLE',
  CONFIGURATION_READ_UNAVAILABLE = 'CONFIGURATION_READ_UNAVAILABLE',
  AUTHORITY_INSTALL_UNAVAILABLE = 'AUTHORITY_INSTALL_UNAVAILABLE',
  EXECUTION_AUTHORITY_CONFLICT = 'EXECUTION_AUTHORITY_CONFLICT',
  INCOMPLETE_SOURCE_CONFIGURATION = 'INCOMPLETE_SOURCE_CONFIGURATION',
  INVALID_SUCCESSOR_AUTHORITY = 'INVALID_SUCCESSOR_AUTHORITY',
  BROKER_UNAVAILABLE = 'BROKER_UNAVAILABLE',
}

export enum ChangeCancellationReason {
  NONE = '',
  CHANGE_TERMINAL = 'CHANGE_TERMINAL',
  CHANGE_SUPERSEDED = 'CHANGE_SUPERSEDED',
  CHANGE_RECONCILING = 'CHANGE_RECONCILING',
  CHANGE_RECOVERY_REQUIRED = 'CHANGE_RECOVERY_REQUIRED',
  CHANGE_NOT_CURRENT = 'CHANGE_NOT_CURRENT',
}

/** Presentation states used by the shared status tag. */
export enum GeneralStatus {
  HEALTHY = 'healthy',
  ONLINE = 'online',
  IDLE = 'idle',
  BUSY = 'busy',
  WARNING = 'warning',
  ERROR = 'error',
  OFFLINE = 'offline',
  DRAINING = 'draining',
  FAULT = 'fault',
  PAUSED = 'paused',
  MAINTENANCE = 'maintenance',
  INITIALIZING = 'initializing',
  RESTARTING = 'restarting',
  ACTIVE = 'active',
  SYNCING = 'syncing',
  DISTRIBUTING = 'distributing',
  DELETING = 'deleting',
  DELETED = 'deleted',
  COMPLETED = 'completed',
  IN_PROGRESS = 'in_progress',
  PENDING = 'pending',
  FAILED = 'failed',
  CANCELLED = 'cancelled',
  UNKNOWN = 'unknown',
}
