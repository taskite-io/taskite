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
/**
 * API 类型定义模块
 * 定义 PD 集群管理后台所有 API 响应的 TypeScript 类型
 * 与后端 Controller 的 23 个接口逐一对应
 */

// ============ 通用类型 ============

/** Java long 标识符/共识进度 → 十进制字符串 */
export type Int64String = string;

import type {
  ChangeStatus, WorkerStatus, MirrorStatus, DistributionStatus, JobSubscriptionCreationType,
  JobYieldStatus, JobYieldReason,
  RegistrationStatus, MemberRole, PartitionLifecycle, PartitionStatus,
  PartitionOperationStatus, RaftNodeState, HealthStatus, ChangeType, MemberOperationType, TopologyOperationType, PlanBlockedReason, ChangeCancellationReason,
} from './enums';
export { ChangeStatus, WorkerStatus, MirrorStatus, DistributionStatus } from './enums';

// ============ A01: PD Health ============
export interface PDHealth {
  status: HealthStatus;
  nodeId: string;
  role: MemberRole;
  leaderId: string;
  leaderEndpoint: string;
  host: string;
  port: number;
  grpcPort: number;
  term: Int64String;
  quorum: number;
  peerCount: number;
  commitIndex: Int64String;
  lastApplied: Int64String;
  lastLogIndex: Int64String;
}

// ============ A02: PD Topology ============
export interface PDMember {
  memberId: string;
  endpoint: string;
  role: MemberRole;
  self: boolean;
  host: string;
  httpPort: number;
  grpcPort: number;
  raftPort: number;
  status: RegistrationStatus;
  incarnationId?: string;
  endpointEpoch?: Int64String;
  commitIndex?: Int64String;
  lastApplied?: Int64String;
  lastLogIndex?: Int64String;
  readyForPromotion?: boolean;
}

export interface PDTopology {
  localMemberId: string;
  leaderId: string;
  leaderEndpoint: string;
  term: Int64String;
  quorum: number;
  members: PDMember[];
}

// ============ A03/A04: PD Configuration ============
export interface PDConfiguration {
  epoch: Int64String;
  target: {
    count: number;
    voters: string[];
  };
  actual: {
    leaderId: string;
    term: Int64String;
    voters: string[];
    learners: string[];
  };
  activeChangeId?: string;
}

export interface PDMutationRequest {
  add?: string[];
  remove?: string[];
  count?: number;
}

// ============ A05/A06: PD Changes ============
export interface PDMemberOperation {
  type: MemberOperationType;
  memberId: string;
  status: ChangeStatus;
  barrierIndex?: Int64String;
  error?: string;
}

export interface PDChangeDetail {
  changeId: string;
  status: ChangeStatus;
  error?: string;
  sourceEpoch: Int64String;
  targetEpoch: Int64String;
  targetCount: number;
  targetVoters: string[];
  actualVoters: string[];
  actualLearners: string[];
  currentOperation?: PDMemberOperation;
  operations: PDMemberOperation[];
  plannedChanges: PDMemberOperation[];
  pendingChanges: PDMemberOperation[];
}

export interface PDChangeAccepted {
  changeId: string;
  status: ChangeStatus;
  sourceEpoch?: Int64String;
  targetEpoch?: Int64String;
  targetCount?: number;
  targetVoters?: string[];
}

// ============ A07: Workers ============
export interface WorkerRegistration {
  clusterId: string;
  tenantId: string;
  workerId: string;
  workerLeaseToken: Int64String;
  leaseDeadlineEpochMillis: Int64String;
  name: string;
  jobType: string;
  maxJobsActive: number;
  activeJobs: number;
  concurrency: number;
  timeoutMillis: Int64String;
  requestTimeoutMillis: Int64String;
  pollIntervalMillis: Int64String;
  fetchVariables: string[];
  clientVersion: string;
  registeredAt: string;
  lastRenewal: string;
  statusChangedAt: string;
  renewalTimeoutMillis: Int64String;
  status: WorkerStatus;
}

export interface WorkerListResponse {
  generatedAt: string;
  total: number;
  online: number;
  offline: number;
  workers: WorkerRegistration[];
}

export interface JobYieldPartition {
  partitionId: number;
  status: JobYieldStatus;
  releasedJobCount: Int64String;
  attempts: number;
  lastAttemptAtEpochMillis: Int64String;
  nextAttemptAtEpochMillis: Int64String;
  acknowledgedAtEpochMillis: Int64String;
  lastError: string;
  pauseReason: string;
}

export interface JobYield {
  yieldId: Int64String;
  clusterId: string;
  tenantId: string;
  workerId: string;
  workerName: string;
  jobType: string;
  workerLeaseToken: Int64String;
  reason: JobYieldReason;
  status: JobYieldStatus;
  createdAtEpochMillis: Int64String;
  completedAtEpochMillis: Int64String;
  releasedJobCount: Int64String;
  totalPartitions: number;
  acknowledgedPartitions: number;
  workerSnapshot: WorkerRegistration;
  partitions: JobYieldPartition[];
}

export interface JobYieldResponse {
  generatedAt: string;
  total: number;
  yields: JobYield[];
}

// ============ A08/A09/A10/A11/A12: Broker Clusters ============
export interface ClusterSummary {
  clusterId: string;
  clusterKey: number;
  clusterSize: number;
  partitionSize: number;
  replicationFactor: number;
  registeredBrokerCount: number;
  onlineBrokerCount: number;
}

export interface ClusterListResponse {
  count: number;
  clusters: ClusterSummary[];
}

export interface CreateClusterRequest {
  clusterId: string;
  clusterSize: number;
  partitionSize: number;
  replicationFactor: number;
}

export interface CreateClusterResponse extends ClusterConfiguration {
  created: boolean;
}

export interface ClusterConfiguration {
  clusterId: string;
  clusterKey: number;
  clusterSize: number;
  partitionSize: number;
  replicationFactor: number;
  voterRoster?: string[];
}

export interface ClusterPatchRequest {
  brokers?: {
    add?: string[];
    remove?: string[];
    count?: number;
  };
  partitions?: {
    count?: number;
    replicationFactor?: number;
  };
}

// ============ A13: Broker Topology ============
export interface BrokerPartitionReport {
  partitionId: number;
  role: MemberRole;
  term: Int64String;
  lifecycleState: PartitionLifecycle;
  matchIndex: Int64String;
  lag: Int64String;
  readyForPromotion: boolean;
  lastOpId: string;
  lastOpStatus: PartitionOperationStatus;
  priority: number;
  recoveryEpoch: Int64String;
  election?: BrokerPartitionElection;
}

export interface BrokerPartitionElection {
  configurationIndex: Int64String;
  configurationDigest: string;
  desiredPriority: number;
  targetPriority: number;
  nodeState: RaftNodeState;
  leaderId: string;
  serviceReady: boolean;
  observedAtEpochMillis: Int64String;
}

export interface BrokerInfo {
  brokerId: string;
  version: string;
  host: string;
  port: number;
  raftPort: number;
  registeredAt: string;
  lastRenewal: string;
  renewalTimeoutMillis: Int64String;
  status: RegistrationStatus;
  partitions: BrokerPartitionReport[];
}

export interface PartitionPlacement {
  partitionId: number;
  leaderId: string;
  term: Int64String;
  recoveryEpoch: Int64String;
  status?: PartitionStatus;
  serviceReady?: boolean;
  replicas: string[];
  learners: string[];
}

export interface BrokerTopology {
  clusterId: string;
  version: string;
  updatedAt: string;
  clusterSize: number;
  partitionSize: number;
  replicationFactor: number;
  brokers: BrokerInfo[];
  partitions: PartitionPlacement[];
}

// ============ A14: Rebalance ============
export interface RebalanceResponse {
  changeId?: string;
  status?: ChangeStatus;
}

// ============ A15: Quorum Recovery ============
export interface QuorumRecoveryRequest {
  confirm: 'true';
  fencingProof: string;
  survivor?: string;
}

export interface QuorumRecoveryResponse {
  changeId: string;
  partitionId: number;
  survivor: string;
  recoveryEpoch: Int64String;
  status: ChangeStatus;
}

// ============ A16/A17/A18: Broker Changes ============
export interface ChangeTopologyItem {
  partitionId: number;
  status: PartitionStatus;
  desiredVoters: string[];
  currentVoters: string[];
  learners: string[];
  desiredPrimary: string;
  priorities: Record<string, number>;
}

export interface PlannedOperation {
  opId: string;
  type: TopologyOperationType;
  targetBroker: string;
  priority: number;
}

export interface PendingItem {
  changeId?: string;
  terminal?: boolean;
  executionGeneration?: Int64String;
  authorityIndex?: Int64String;
  rootChangeId?: string;
  supersedes?: string;
  supersededBy?: string;
  blockedReason?: PlanBlockedReason;
  blockedSince?: string;
  partitionId: number;
  status: ChangeStatus;
  nextOperationIndex: number;
  operationCount: number;
  currentOperation?: PlannedOperation;
  remainingOperations: PlannedOperation[];
  startedAt: string;
  deadline: string;
}

export interface BrokerChangeDetail {
  changeId: string;
  status: ChangeStatus;
  changeType?: ChangeType;
  cancellable?: boolean;
  cancellationReason?: ChangeCancellationReason;
  currentTopology: ChangeTopologyItem[];
  expectedTopology: ChangeTopologyItem[];
  plannedChanges: PlannedOperation[];
  pendingChanges: PendingItem[];
  executionHistory?: PendingItem[];
}

export interface CancelChangeResponse {
  clusterId: string;
  changeId: string;
  status: ChangeStatus.CANCELLED;
}

// ============ A19/A20/A21/A22: Resource Mirrors ============
export interface MirrorItem {
  mirrorKey: Int64String;
  sourceClusterId: string;
  targetClusterId: string;
  status: MirrorStatus;
  created?: boolean;
}

export interface MirrorListResponse {
  mirrors: MirrorItem[];
}

export interface CreateMirrorRequest {
  sourceClusterId: string;
  targetClusterId: string;
}

// ============ A23: Distribution ============
export interface DistributionTarget {
  clusterId: string;
  expectedPartitions: number;
  acknowledgedPartitions: number;
  pendingPartitions: number[];
  status: DistributionStatus;
  confirmedPartitionIds: number[];
  mirrorKey?: Int64String;
}

export interface JobSubscription {
  clusterId: string;
  tenantId: string;
  name: string;
  jobType: string;
  variables: string;
  retries: number;
  priority: number;
}

export interface CreateJobSubscriptionRequest extends Omit<JobSubscription, 'tenantId' | 'retries' | 'priority'> {
  tenantId?: string;
  retries?: number;
  priority?: number;
}

export interface CreateJobSubscriptionResponse {
  distributionKey: Int64String;
}

export type TimerSubscriptionType = 'DURATION' | 'CYCLE' | 'CRON';

export interface CreateTimerSubscriptionRequest {
  clusterId: string;
  tenantId?: string;
  name: string;
  type: TimerSubscriptionType;
  expression: string;
  jobTypes: string[];
  variables: string;
}

export interface CreateTimerSubscriptionResponse {
  timerSubscriptionKey: Int64String;
  dueDate: Int64String;
}

export interface TimerSubscription extends CreateTimerSubscriptionRequest {
  timerSubscriptionKey: Int64String;
  tenantId: string;
  timeZone: string;
  dueDate: Int64String;
  repetitions: number;
  occurrence: Int64String;
}

export interface TimerSubscriptionListResponse {
  total: number;
  subscriptions: TimerSubscription[];
}

export interface DistributionDetail {
  distributionKey: Int64String;
  subscriptionKey?: Int64String;
  deletionDistributionKey?: Int64String;
  sourceClusterId: string;
  clusterId: string;
  creationType: JobSubscriptionCreationType;
  valueType: string;
  status: DistributionStatus;
  targets: DistributionTarget[];
  name: string;
  jobType: string;
  tenantId: string;
  subscription?: JobSubscription;
  generation: Int64String;
  lastRedistributedAt: Int64String;
}

export interface JobSubscriptionListResponse {
  total: number;
  subscriptions: DistributionDetail[];
}

export interface JobSubscriptionRedistributionAccepted {
  distributionKey: Int64String;
  generation: Int64String;
  lastRedistributedAt: Int64String;
  status: DistributionStatus;
}

// ============ A24: Bootstrap ============
export interface ConsoleBootstrap {
  productName: string;
  consoleBasePath: string;
  apiBasePath: string;
  buildVersion: string;
  deploymentId: string;
  nodeId: string;
}

// ============ 通用错误 ============
export interface ApiErrorBody {
  error?: string;
  message?: string;
  leaderEndpoint?: string;
  pendingChangeIds?: string[];
  clusterId?: string;
}
