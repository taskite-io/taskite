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
type Shape = 's' | 'i' | 'l' | 'b' | {array: Shape} | {optional: Shape} | {map: Shape} | {[key: string]: Shape};
import {
  MemberRole, RegistrationStatus, WorkerStatus, ChangeStatus, DistributionStatus, MirrorStatus,
  PartitionLifecycle, PartitionOperationStatus, PartitionStatus, RaftNodeState, JobSubscriptionCreationType,
  ChangeType, PlanBlockedReason, ChangeCancellationReason, MemberOperationType, TopologyOperationType,
  JobYieldStatus, JobYieldReason,
} from './enums';

const enumFields: Record<string, readonly string[]> = {
  role: Object.values(MemberRole),
  status: [...Object.values(RegistrationStatus), ...Object.values(WorkerStatus), ...Object.values(ChangeStatus),
    ...Object.values(DistributionStatus), ...Object.values(MirrorStatus), ...Object.values(PartitionStatus),
    ...Object.values(JobYieldStatus)],
  reason: Object.values(JobYieldReason),
  lifecycleState: Object.values(PartitionLifecycle),
  lastOpStatus: Object.values(PartitionOperationStatus),
  nodeState: Object.values(RaftNodeState),
  creationType: Object.values(JobSubscriptionCreationType),
  changeType: Object.values(ChangeType),
  blockedReason: Object.values(PlanBlockedReason),
  cancellationReason: Object.values(ChangeCancellationReason),
  type: [...Object.values(MemberOperationType), ...Object.values(TopologyOperationType)],
};
const array = (value: Shape): Shape => ({array: value});
const optional = (value: Shape): Shape => ({optional: value});
const strings = array('s'), ints = array('i');
const configuration: Shape = {clusterId: 's', clusterSize: 'i', partitionSize: 'i', replicationFactor: 'i'};
const memberOperation: Shape = {type: 's', memberId: 's', status: 's', barrierIndex: optional('l'), error: optional('s')};
const operation: Shape = {opId: 's', type: 's', targetBroker: 's', priority: 'i'};
const placement: Shape = {
  partitionId: 'i', status: optional('s'), desiredVoters: strings, currentVoters: strings,
  learners: strings, desiredPrimary: optional('s'), priorities: {map: 'i'},
};
const election: Shape = {
  configurationIndex: 'l', configurationDigest: 's', desiredPriority: 'i', targetPriority: 'i',
  nodeState: 's', leaderId: 's', serviceReady: 'b', observedAtEpochMillis: 'l',
};
const report: Shape = {
  partitionId: 'i', role: 's', term: 'l', lifecycleState: 's', matchIndex: 'l', lag: 'l',
  readyForPromotion: 'b', lastOpId: 's', lastOpStatus: 's', priority: 'i', recoveryEpoch: 'l', election: optional(election),
};
const mirror: Shape = {mirrorKey: 'l', sourceClusterId: 's', targetClusterId: 's', status: 's', created: optional('b')};
const distribution: Shape = {
  distributionKey: 'l', sourceClusterId: 's', clusterId: 's', creationType: 's', valueType: 's', status: 's',
  subscriptionKey: optional('l'), deletionDistributionKey: optional('l'),
  name: 's', jobType: 's', tenantId: 's',
  subscription: optional({
    clusterId: 's', tenantId: 's', name: 's', jobType: 's',
    variables: 's', retries: 'i', priority: 'i',
  }), generation: 'l', lastRedistributedAt: 'l',
  targets: array({clusterId: 's', expectedPartitions: 'i', acknowledgedPartitions: 'i',
    pendingPartitions: ints, confirmedPartitionIds: ints, mirrorKey: optional('l'), status: 's'}),
};
const pdChange: Shape = {
  changeId: 's', status: 's', error: optional('s'), sourceEpoch: 'l', targetEpoch: 'l', targetCount: 'i',
  targetVoters: strings, actualVoters: strings, actualLearners: strings, currentOperation: optional(memberOperation),
  operations: array(memberOperation), plannedChanges: array(memberOperation), pendingChanges: array(memberOperation),
};
const brokerChange: Shape = {
  changeId: 's', status: 's', currentTopology: array(placement), expectedTopology: array(placement),
  changeType: optional('s'), cancellable: optional('b'), cancellationReason: optional('s'),
  plannedChanges: array(operation), pendingChanges: array({
    partitionId: 'i', status: 's', nextOperationIndex: 'i', operationCount: 'i', currentOperation: optional(operation),
    remainingOperations: array(operation), startedAt: optional('s'), deadline: optional('s'),
    changeId: optional('s'), terminal: optional('b'), executionGeneration: optional('l'),
    authorityIndex: optional('l'), rootChangeId: optional('s'), supersedes: optional('s'),
    supersededBy: optional('s'), blockedReason: optional('s'), blockedSince: optional('s'),
  }),
  executionHistory: optional(array({
    changeId: 's', partitionId: 'i', status: 's', terminal: 'b',
    executionGeneration: 'l', authorityIndex: 'l', rootChangeId: 's',
    supersedes: 's', supersededBy: 's', blockedReason: 's', blockedSince: optional('s'),
    nextOperationIndex: 'i', operationCount: 'i', currentOperation: optional(operation),
    remainingOperations: array(operation), startedAt: optional('s'), deadline: optional('s'),
  })),
};
const worker: Shape = {
  clusterId: 's', tenantId: 's', workerId: 's', name: 's', jobType: 's', maxJobsActive: 'i', activeJobs: 'i',
  concurrency: 'i', timeoutMillis: 'l', requestTimeoutMillis: 'l', pollIntervalMillis: 'l', fetchVariables: strings,
  clientVersion: 's', registeredAt: 's', lastRenewal: 's', statusChangedAt: 's', renewalTimeoutMillis: 'l', status: 's',
  workerLeaseToken: 'l', leaseDeadlineEpochMillis: 'l',
};
const jobYield: Shape = {
  yieldId: 'l', clusterId: 's', tenantId: 's', workerId: 's', workerName: 's', jobType: 's',
  workerLeaseToken: 'l', reason: 's', status: 's', createdAtEpochMillis: 'l', completedAtEpochMillis: 'l',
  releasedJobCount: 'l', totalPartitions: 'i', acknowledgedPartitions: 'i', workerSnapshot: worker,
  partitions: array({partitionId: 'i', status: 's', releasedJobCount: 'l', attempts: 'i',
    lastAttemptAtEpochMillis: 'l', nextAttemptAtEpochMillis: 'l', acknowledgedAtEpochMillis: 'l',
    lastError: 's', pauseReason: 's'}),
};
function check(value: unknown, shape: Shape, path = 'response'): void {
  const invalid = () => { throw new Error(`响应字段 ${path} 类型不符合契约`); };
  if (typeof shape === 'string') {
    const valid = shape === 's' ? typeof value === 'string' : shape === 'b' ? typeof value === 'boolean'
      : shape === 'i' ? typeof value === 'number' && Number.isInteger(value) && value >= -2147483648 && value <= 2147483647
      : typeof value === 'string' && /^-?\d+$/.test(value) && BigInt(value) >= -(1n << 63n) && BigInt(value) < (1n << 63n);
    if (!valid) invalid();
    return;
  }
  if ('optional' in shape) { if (value !== undefined && value !== null) check(value, shape.optional, path); return; }
  if ('array' in shape) {
    if (!Array.isArray(value)) return invalid();
    value.forEach((item, index) => check(item, shape.array, `${path}[${index}]`)); return;
  }
  if (!value || typeof value !== 'object' || Array.isArray(value)) return invalid();
  const object = value as Record<string, unknown>;
  if ('map' in shape) { Object.entries(object).forEach(([key, item]) => check(item, shape.map, `${path}.${key}`)); return; }
  Object.entries(shape).forEach(([key, field]) => {
    check(object[key], field, `${path}.${key}`);
    // Normalize nullable optional DTO fields without inventing any business value.
    if (object[key] === null && typeof field === 'object' && 'optional' in field) delete object[key];
    // Decode known enum names once at the API boundary; retain future names for display.
    const value = object[key];
    if (typeof value === 'string' && enumFields[key]) {
      const normalized = value.trim().toUpperCase();
      if (enumFields[key].includes(normalized)) object[key] = normalized;
    }
  });
}
export function validateResponse(path: string, method: string, body: unknown): void {
  const route = path.split('?')[0];
  let shape: Shape | undefined;
  if (method !== 'GET') {
    if (method === 'POST' && route === '/broker/clusters') shape = {...configuration as object, created: 'b'};
    else if (method === 'POST' && route === '/broker/job-subscriptions') shape = {distributionKey: 'l'};
    else if (method === 'POST' && route === '/broker/timer-subscriptions') shape = {timerSubscriptionKey: 'l', dueDate: 'l'};
    else if (method === 'DELETE' && /^\/broker\/job-subscriptions\/[^/]+$/.test(route)) shape = {distributionKey: 'l'};
    else if (method === 'DELETE' && /^\/broker\/timer-subscriptions\/[^/]+$/.test(route)) shape = {timerSubscriptionKey: 'l'};
    else if (method === 'POST' && route === '/broker/job-subscription-mirrors') shape = mirror;
    else if (method === 'POST' && /^\/broker\/job-subscriptions\/[^/]+\/redistribute$/.test(route))
      shape = {distributionKey: 'l', generation: 'l', lastRedistributedAt: 'l', status: 's'};
    else if (route.endsWith('/quorum-recovery')) shape = {clusterId: 's', changeId: 's', partitionId: 'i', survivor: 's', recoveryEpoch: 'l', status: 's'};
    else shape = {changeId: 's', status: optional('s')};
  } else if (route === '/console/bootstrap') shape = {
    productName: 's', consoleBasePath: 's', apiBasePath: 's', buildVersion: 's', deploymentId: 's', nodeId: 's',
  };
  else if (route === '/pd/health') shape = {
    status: 's', nodeId: 's', role: 's', leaderId: optional('s'), leaderEndpoint: optional('s'),
    host: 's', port: 'i', grpcPort: 'i', term: 'l', quorum: 'i', peerCount: 'i', commitIndex: 'l', lastApplied: 'l', lastLogIndex: 'l',
  };
  else if (route === '/pd/topology') shape = {
    localMemberId: 's', leaderId: optional('s'), leaderEndpoint: optional('s'), term: 'l', quorum: 'i',
    members: array({memberId: 's', endpoint: optional('s'), role: 's', self: 'b', host: optional('s'), httpPort: optional('i'),
      grpcPort: optional('i'), raftPort: optional('i'), status: optional('s'), incarnationId: optional('s'),
      endpointEpoch: optional('l'), commitIndex: optional('l'), lastApplied: optional('l'), lastLogIndex: optional('l'), readyForPromotion: optional('b')}),
  };
  else if (route === '/pd/configuration') shape = {epoch: 'l', target: {count: 'i', voters: strings},
    actual: {leaderId: optional('s'), term: 'l', voters: strings, learners: strings}, activeChangeId: optional('s')};
  else if (route.startsWith('/pd/changes/')) shape = pdChange;
  else if (route === '/workers') shape = {generatedAt: 's', total: 'i', online: 'i', offline: 'i', workers: array(worker)};
  else if (route === '/workers/yields') shape = {generatedAt: 's', total: 'i', yields: array(jobYield)};
  else if (route.startsWith('/workers/yields/')) shape = jobYield;
  else if (route === '/broker/clusters') shape = {count: 'i', clusters: array({...configuration as object, registeredBrokerCount: 'i', onlineBrokerCount: 'i'})};
  else if (/^\/broker\/clusters\/[^/]+\/configuration$/.test(route)) shape = configuration;
  else if (/^\/broker\/clusters\/[^/]+\/topology$/.test(route)) shape = {
    ...configuration as object, version: 'l', updatedAt: 's',
    brokers: array({brokerId: 's', version: 's', host: 's', port: 'i', raftPort: 'i', registeredAt: 's', lastRenewal: 's',
      renewalTimeoutMillis: 'l', status: 's', partitions: array(report)}),
    partitions: array({partitionId: 'i', leaderId: optional('s'), term: 'l', recoveryEpoch: 'l', replicas: strings, learners: strings}),
  };
  else if (/^\/broker\/clusters\/[^/]+\/changes\/[^/]+$/.test(route)) shape = brokerChange;
  else if (route === '/broker/job-subscription-mirrors') shape = {mirrors: array(mirror)};
  else if (route.startsWith('/broker/job-subscription-mirrors/')) shape = mirror;
  else if (route === '/broker/job-subscriptions') shape = {total: 'i', subscriptions: array(distribution)};
  else if (route === '/broker/timer-subscriptions') shape = {total: 'i', subscriptions: array({
    timerSubscriptionKey: 'l', clusterId: 's', tenantId: 's', name: 's', type: 's',
    expression: 's', jobTypes: strings, variables: 's', timeZone: 's',
    dueDate: 'l', repetitions: 'i', occurrence: 'l',
  })};
  else if (route.startsWith('/broker/job-subscriptions/')) shape = distribution;
  if (!shape) throw new Error(`没有配置端点响应契约：${method} ${route}`);
  check(body, shape);
}
