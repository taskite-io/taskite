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
package io.taskite.placementdriver.processing;

import io.taskite.placementdriver.PlacementDriverConfiguration;
import io.taskite.placementdriver.RegistrationPolicy;
import io.taskite.placementdriver.broker.cluster.BrokerClusterChangePlanningService;
import io.taskite.placementdriver.processing.broker.topology.BrokerMemberChangePlanner;
import io.taskite.placementdriver.processing.member.MemberChangePlanner;
import io.taskite.placementdriver.processing.placementdriver.PlacementDriverMemberChangePlanner;
import java.util.List;
import java.util.Objects;

/** Owned collaborators used while constructing placement-driver processors. */
public record PlacementDriverProcessorDependencies(
    BrokerClusterChangePlanningService clusterChangePlanningService,
    List<MemberChangePlanner> memberChangePlanners,
    RegistrationPolicy registrationPolicy,
    PlacementDriverCommandWriter commandWriter) {
  private static final String MISSING_REGISTRATION_POLICY =
      "Expected to construct PD processor dependencies, but registrationPolicy is null";
  private static final String MISSING_COMMAND_WRITER =
      "Expected to construct PD processor dependencies, but commandWriter is null";

  public PlacementDriverProcessorDependencies {
    memberChangePlanners = List.copyOf(memberChangePlanners);
    Objects.requireNonNull(registrationPolicy, MISSING_REGISTRATION_POLICY);
    Objects.requireNonNull(commandWriter, MISSING_COMMAND_WRITER);
  }

  public static PlacementDriverProcessorDependencies create() {
    return create(
        new RegistrationPolicy(
            PlacementDriverConfiguration.DEFAULT_BROKER_LEASE_TIMEOUT,
            PlacementDriverConfiguration.DEFAULT_WORKER_LEASE_TIMEOUT,
            PlacementDriverConfiguration.DEFAULT_WORKER_OFFLINE_RETENTION));
  }

  public static PlacementDriverProcessorDependencies create(
      final RegistrationPolicy registrationPolicy) {
    final BrokerClusterChangePlanningService planningService =
        new BrokerClusterChangePlanningService();
    return new PlacementDriverProcessorDependencies(
        planningService,
        List.of(
            new BrokerMemberChangePlanner(planningService),
            new PlacementDriverMemberChangePlanner()),
        registrationPolicy,
        new PlacementDriverCommandWriter());
  }
}
