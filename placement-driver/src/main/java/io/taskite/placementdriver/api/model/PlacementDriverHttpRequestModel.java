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
package io.taskite.placementdriver.api.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

/**
 * Request-body DTOs for the write endpoints of the placement-driver admin API. RESTful convention:
 * resource identifiers ({@code clusterId}, {@code partitionId}, …) travel as path variables, while
 * the parameters that <em>modify the action</em> (sizing values, the recovery confirmation/proof)
 * travel in the JSON request body rather than as query parameters. Deserialized by Jackson.
 *
 * <p>Fields are deliberately kept as raw {@code String} so the endpoint layer can emit its bespoke
 * {@code 400} envelopes (e.g. {@code INVALID_CLUSTER_CONFIG}, {@code CONFIRMATION_REQUIRED}, {@code
 * FENCING_PROOF_REQUIRED}) rather than letting Jackson reject a malformed value first. Unknown
 * properties are ignored so callers can send a superset without a 400.
 */
public final class PlacementDriverHttpRequestModel {
  private PlacementDriverHttpRequestModel() {}

  /** Body of {@code POST /clusters}. All sizing fields are required. */
  public record CreateClusterRequest(
      String clusterId, Integer clusterSize, Integer partitionSize, Integer replicationFactor) {}

  public record CreateJobSubscriptionMirrorRequest(
      String sourceClusterId, String targetClusterId) {}

  public record CreateJobSubscriptionRequest(
      String clusterId,
      String tenantId,
      String name,
      String jobType,
      String variables,
      Integer retries,
      Integer priority) {}

  public record CreateTimerSubscriptionRequest(
      String clusterId,
      String tenantId,
      String name,
      String type,
      String expression,
      List<String> jobTypes,
      String variables) {}

  public record RedistributeJobSubscriptionRequest(String expectedGeneration) {}

  public record BrokerPatch(List<String> add, List<String> remove, Integer count) {}

  public record PartitionPatch(Integer count, Integer replicationFactor) {}

  public record ClusterConfigurationPatch(BrokerPatch brokers, PartitionPatch partitions) {}

  public record PlacementDriverConfigurationPatch(
      List<String> add, List<String> remove, Integer count) {}

  /**
   * Body of {@code POST /clusters/{clusterId}/partitions/{partitionId}/quorum-recovery}. {@code
   * confirm} must be {@code "true"} (double confirmation) and {@code fencingProof} must be
   * non-empty; {@code survivor} is optional (PD chooses the highest-matchIndex online voter when
   * omitted). Validation and the resulting error envelopes live behind the endpoint manager.
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public record RecoverQuorumRequest(String confirm, String fencingProof, String survivor) {}
}
