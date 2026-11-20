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
package io.taskite.placementdriver.api.rest;

import io.taskite.placementdriver.api.PlacementDriverEndpointManager;
import io.taskite.placementdriver.api.model.PlacementDriverHttpRequestModel.ClusterConfigurationPatch;
import io.taskite.placementdriver.api.model.PlacementDriverHttpRequestModel.CreateClusterRequest;
import io.taskite.placementdriver.api.model.PlacementDriverHttpRequestModel.CreateJobSubscriptionMirrorRequest;
import io.taskite.placementdriver.api.model.PlacementDriverHttpRequestModel.CreateJobSubscriptionRequest;
import io.taskite.placementdriver.api.model.PlacementDriverHttpRequestModel.CreateTimerSubscriptionRequest;
import io.taskite.placementdriver.api.model.PlacementDriverHttpRequestModel.PlacementDriverConfigurationPatch;
import io.taskite.placementdriver.api.model.PlacementDriverHttpRequestModel.RecoverQuorumRequest;
import io.taskite.placementdriver.api.model.PlacementDriverHttpRequestModel.RedistributeJobSubscriptionRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.async.DeferredResult;

/**
 * Spring MVC front end for the placement-driver admin/observability API. Every endpoint delegates
 * request routing and response adaptation to {@link PlacementDriverEndpointManager}.
 *
 * <p>Resource identifiers are path variables; the values that <em>modify</em> a write (config
 * sizing, quorum-recovery confirmation/proof) are carried in a JSON {@link RequestBody} rather than
 * as query parameters (see {@link PlacementDriverHttpRequestModel}). Validation-bearing body/path
 * values are taken as raw {@code String} so the endpoint manager can emit the established error
 * envelopes rather than letting Spring reject a malformed value first.
 */
@RestController
@RequestMapping("/api/v1")
public class PlacementDriverAdminController {
  private final PlacementDriverEndpointManager endpointManager;

  public PlacementDriverAdminController(final PlacementDriverEndpointManager endpointManager) {
    this.endpointManager = endpointManager;
  }

  @GetMapping("/pd/health")
  public DeferredResult<ResponseEntity<Object>> health() {
    return endpointManager.queryHealth();
  }

  @GetMapping("/pd/topology")
  public DeferredResult<ResponseEntity<Object>> placementDriverTopology() {
    return endpointManager.queryPlacementDriverTopology();
  }

  @GetMapping("/pd/configuration")
  public DeferredResult<ResponseEntity<Object>> placementDriverConfiguration() {
    return endpointManager.queryPlacementDriverConfiguration();
  }

  @PatchMapping("/pd/configuration")
  public DeferredResult<ResponseEntity<Object>> patchPlacementDriverConfiguration(
      @RequestBody(required = false) final PlacementDriverConfigurationPatch request) {
    return endpointManager.patchPlacementDriverConfiguration(request);
  }

  @GetMapping("/pd/changes/active")
  public DeferredResult<ResponseEntity<Object>> activePlacementDriverChange() {
    return endpointManager.queryPlacementDriverMemberChange("");
  }

  @GetMapping("/pd/changes/{changeId}")
  public DeferredResult<ResponseEntity<Object>> placementDriverChangeStatus(
      @PathVariable("changeId") final String changeId) {
    return endpointManager.queryPlacementDriverMemberChange(changeId);
  }

  @GetMapping("/workers")
  public DeferredResult<ResponseEntity<Object>> workers(
      @RequestParam(name = "cluster", required = false) final String clusterId,
      @RequestParam(name = "tenant", required = false) final String tenantId,
      @RequestParam(name = "name", required = false) final String name) {
    return endpointManager.queryWorkers(clusterId, tenantId, name);
  }

  @PostMapping("/broker/clusters")
  public DeferredResult<ResponseEntity<Object>> createCluster(
      @RequestBody(required = false) final CreateClusterRequest request) {
    return endpointManager.createBrokerCluster(request);
  }

  @GetMapping("/workers/yields")
  public DeferredResult<ResponseEntity<Object>> jobYields(
      @RequestParam(name = "cluster", required = false) String clusterId,
      @RequestParam(name = "tenant", required = false) String tenantId,
      @RequestParam(name = "workerId", required = false) String workerId,
      @RequestParam(name = "workerLeaseToken", required = false) String workerLeaseToken) {
    return endpointManager.queryJobYields(clusterId, tenantId, workerId, workerLeaseToken, null);
  }

  @GetMapping("/workers/yields/{yieldId}")
  public DeferredResult<ResponseEntity<Object>> jobYield(@PathVariable("yieldId") String yieldId) {
    return endpointManager.queryJobYields(null, null, null, null, yieldId);
  }

  @GetMapping("/broker/clusters")
  public DeferredResult<ResponseEntity<Object>> clusters() {
    return endpointManager.queryBrokerClusters();
  }

  @PostMapping("/broker/job-subscription-mirrors")
  public DeferredResult<ResponseEntity<Object>> createJobSubscriptionMirror(
      @RequestBody(required = false) final CreateJobSubscriptionMirrorRequest request) {
    return endpointManager.createJobSubscriptionMirror(request);
  }

  @GetMapping("/broker/job-subscription-mirrors")
  public DeferredResult<ResponseEntity<Object>> jobSubscriptionMirrors(
      @RequestParam(name = "sourceClusterId", required = false) final String sourceClusterId,
      @RequestParam(name = "targetClusterId", required = false) final String targetClusterId) {
    return endpointManager.queryJobSubscriptionMirrors(sourceClusterId, targetClusterId);
  }

  @GetMapping("/broker/job-subscription-mirrors/{mirrorKey}")
  public DeferredResult<ResponseEntity<Object>> jobSubscriptionMirror(
      @PathVariable("mirrorKey") final String mirrorKey) {
    return endpointManager.queryJobSubscriptionMirror(mirrorKey);
  }

  @DeleteMapping("/broker/job-subscription-mirrors/{mirrorKey}")
  public DeferredResult<ResponseEntity<Object>> deleteJobSubscriptionMirror(
      @PathVariable("mirrorKey") final String mirrorKey) {
    return endpointManager.deleteJobSubscriptionMirror(mirrorKey);
  }

  @GetMapping("/broker/job-subscriptions/{distributionKey}")
  public DeferredResult<ResponseEntity<Object>> jobSubscriptionDistribution(
      @PathVariable("distributionKey") final String distributionKey) {
    return endpointManager.queryJobSubscriptionDistribution(distributionKey);
  }

  @PostMapping("/broker/job-subscriptions")
  public DeferredResult<ResponseEntity<Object>> createJobSubscription(
      @RequestBody(required = false) final CreateJobSubscriptionRequest request) {
    return endpointManager.createJobSubscription(request);
  }

  @PostMapping("/broker/timer-subscriptions")
  public DeferredResult<ResponseEntity<Object>> createTimerSubscription(
      @RequestBody(required = false) final CreateTimerSubscriptionRequest request) {
    return endpointManager.createTimerSubscription(request);
  }

  @GetMapping("/broker/timer-subscriptions")
  public DeferredResult<ResponseEntity<Object>> timerSubscriptions() {
    return endpointManager.queryTimerSubscriptions();
  }

  @DeleteMapping("/broker/job-subscriptions/{subscriptionKey}")
  public DeferredResult<ResponseEntity<Object>> deleteJobSubscription(
      @PathVariable("subscriptionKey") final String subscriptionKey,
      @RequestParam(name = "clusterId", required = false) final String clusterId,
      @RequestParam(name = "tenantId", required = false) final String tenantId) {
    return endpointManager.deleteJobSubscription(subscriptionKey, clusterId, tenantId);
  }

  @DeleteMapping("/broker/timer-subscriptions/{timerSubscriptionKey}")
  public DeferredResult<ResponseEntity<Object>> deleteTimerSubscription(
      @PathVariable("timerSubscriptionKey") final String timerSubscriptionKey,
      @RequestParam(name = "clusterId", required = false) final String clusterId,
      @RequestParam(name = "tenantId", required = false) final String tenantId) {
    return endpointManager.deleteTimerSubscription(timerSubscriptionKey, clusterId, tenantId);
  }

  @GetMapping("/broker/job-subscriptions")
  public DeferredResult<ResponseEntity<Object>> jobSubscriptionDistributions(
      @RequestParam(name = "jobType", required = false) final String jobType,
      @RequestParam(name = "distributionKey", required = false) final String distributionKey,
      @RequestParam(name = "clusterId", required = false) final String clusterId) {
    return endpointManager.queryJobSubscriptionDistributions(jobType, distributionKey, clusterId);
  }

  @PostMapping("/broker/job-subscriptions/{distributionKey}/redistribute")
  public DeferredResult<ResponseEntity<Object>> redistributeJobSubscription(
      @PathVariable("distributionKey") final String distributionKey,
      @RequestBody(required = false) final RedistributeJobSubscriptionRequest request) {
    return endpointManager.redistributeJobSubscription(distributionKey, request);
  }

  @DeleteMapping("/broker/clusters/{clusterId}")
  public DeferredResult<ResponseEntity<Object>> deleteCluster(
      @PathVariable("clusterId") final String clusterId) {
    return endpointManager.deleteBrokerCluster(clusterId);
  }

  @GetMapping("/broker/clusters/{clusterId}/configuration")
  public DeferredResult<ResponseEntity<Object>> getClusterConfig(
      @PathVariable("clusterId") final String clusterId) {
    return endpointManager.queryBrokerClusterConfiguration(clusterId);
  }

  @PatchMapping("/broker/clusters/{clusterId}/configuration")
  public DeferredResult<ResponseEntity<Object>> patchClusterConfiguration(
      @PathVariable("clusterId") final String clusterId,
      @RequestBody(required = false) final ClusterConfigurationPatch request) {
    return endpointManager.patchBrokerClusterConfiguration(clusterId, request);
  }

  @GetMapping("/broker/clusters/{clusterId}/topology")
  public DeferredResult<ResponseEntity<Object>> topology(
      @PathVariable("clusterId") final String clusterId) {
    return endpointManager.queryBrokerTopology(clusterId);
  }

  @PostMapping("/broker/clusters/{clusterId}/partitions/{partitionId}/quorum-recovery")
  public DeferredResult<ResponseEntity<Object>> recoverQuorum(
      @PathVariable("clusterId") final String clusterId,
      @PathVariable("partitionId") final String partitionId,
      @RequestBody(required = false) final RecoverQuorumRequest request) {
    return endpointManager.recoverBrokerTopologyQuorum(clusterId, partitionId, request);
  }

  @PostMapping("/broker/clusters/{clusterId}/rebalance")
  public DeferredResult<ResponseEntity<Object>> rebalance(
      @PathVariable("clusterId") final String clusterId) {
    return endpointManager.rebalanceBrokerTopology(clusterId);
  }

  @GetMapping("/broker/clusters/{clusterId}/changes/active")
  public DeferredResult<ResponseEntity<Object>> activeChange(
      @PathVariable("clusterId") final String clusterId) {
    return endpointManager.queryBrokerClusterChange(clusterId, "");
  }

  @GetMapping("/broker/clusters/{clusterId}/changes/{changeId}")
  public DeferredResult<ResponseEntity<Object>> changeStatus(
      @PathVariable("clusterId") final String clusterId,
      @PathVariable("changeId") final String changeId) {
    return endpointManager.queryBrokerClusterChange(clusterId, changeId);
  }

  @DeleteMapping("/broker/clusters/{clusterId}/changes/{changeId}")
  public DeferredResult<ResponseEntity<Object>> cancelChange(
      @PathVariable("clusterId") final String clusterId,
      @PathVariable("changeId") final String changeId) {
    return endpointManager.cancelBrokerClusterChange(clusterId, changeId);
  }
}
