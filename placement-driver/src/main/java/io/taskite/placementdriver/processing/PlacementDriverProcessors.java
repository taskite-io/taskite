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

import io.taskite.broker.client.BrokerClient;
import io.taskite.placementdriver.PlacementDriverConfiguration;
import io.taskite.placementdriver.RegistrationPolicy;
import io.taskite.placementdriver.broker.cluster.GrpcBrokerClusterChangeTransport;
import io.taskite.placementdriver.broker.distribution.GrpcBrokerJobSubscriptionTransport;
import io.taskite.placementdriver.broker.distribution.ImmutableStateBrokerDistributionRouteResolver;
import io.taskite.placementdriver.job.GrpcJobYieldTransport;
import io.taskite.placementdriver.processing.broker.cluster.BrokerClusterProcessors;
import io.taskite.placementdriver.processing.broker.distribution.BrokerDistributionScheduler;
import io.taskite.placementdriver.processing.broker.distribution.CommandDistributionDispatchProcessor;
import io.taskite.placementdriver.processing.broker.distribution.CommandDistributionProcessors;
import io.taskite.placementdriver.processing.broker.distribution.CommandDistributionRetryProcessor;
import io.taskite.placementdriver.processing.broker.mirror.JobSubscriptionMirrorProcessors;
import io.taskite.placementdriver.processing.broker.registry.BrokerProcessors;
import io.taskite.placementdriver.processing.broker.timer.DueDateTimerChecker;
import io.taskite.placementdriver.processing.broker.timer.TimerCancelProcessor;
import io.taskite.placementdriver.processing.broker.timer.TimerCreateProcessor;
import io.taskite.placementdriver.processing.broker.timer.TimerTriggerProcessor;
import io.taskite.placementdriver.processing.broker.topology.BrokerTopologyCoordinatorChecker;
import io.taskite.placementdriver.processing.broker.topology.TopologyCoordinateProcessor;
import io.taskite.placementdriver.processing.job.JobYieldAcknowledgeProcessor;
import io.taskite.placementdriver.processing.job.JobYieldCommandSender;
import io.taskite.placementdriver.processing.job.JobYieldScheduler;
import io.taskite.placementdriver.processing.job.JobYieldUpdateProgressProcessor;
import io.taskite.placementdriver.processing.member.MemberProcessors;
import io.taskite.placementdriver.processing.member.PdMemberChangeTransport;
import io.taskite.placementdriver.processing.placementdriver.PlacementDriverClusterProcessors;
import io.taskite.placementdriver.processing.query.TopologyQueryProcessors;
import io.taskite.placementdriver.processing.registry.RegistrationDeadlineChecker;
import io.taskite.placementdriver.processing.topology.MemberExecuteProcessor;
import io.taskite.placementdriver.processing.topology.PartitionExecuteProcessor;
import io.taskite.placementdriver.processing.topology.TopologyMembershipChangeChecker;
import io.taskite.placementdriver.processing.topology.TopologyMembershipChangeExecutor;
import io.taskite.placementdriver.processing.topology.TopologyMembershipChangeProcessors;
import io.taskite.placementdriver.processing.worker.registry.WorkerProcessors;
import io.taskite.placementdriver.state.appliers.PlacementDriverEventAppliers;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.broker.JobYieldRecord;
import io.taskite.protocol.impl.record.value.common.TimerSubscriptionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionListRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionQueryRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRedistributeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRequestRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionDeletionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberExecutionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.mirror.JobSubscriptionMirrorQueryRecord;
import io.taskite.protocol.impl.record.value.placementdriver.mirror.JobSubscriptionMirrorRecord;
import io.taskite.protocol.impl.record.value.placementdriver.partitionchange.PartitionChangeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.partitionchange.PartitionExecutionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerRegistrationRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerTopologyCoordinationRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.ClusterConfigRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.MemberTopologyRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.TopologyQueryRecord;
import io.taskite.protocol.impl.record.value.placementdriver.worker.WorkerRegistrationRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.JobYieldIntent;
import io.taskite.protocol.record.intent.common.TimerIntent;
import io.taskite.protocol.record.intent.placementdriver.BrokerPartitionChangeIntent;
import io.taskite.protocol.record.intent.placementdriver.CommandDistributionIntent;
import io.taskite.protocol.record.intent.placementdriver.JobSubscriptionIntent;
import io.taskite.protocol.record.intent.placementdriver.JobSubscriptionMirrorIntent;
import io.taskite.protocol.record.intent.placementdriver.MemberChangeIntent;
import io.taskite.protocol.record.intent.placementdriver.TopologyIntent;
import io.taskite.state.processing.ProcessingStateStore;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContextImpl;
import io.taskite.streamplatform.api.processor.TypedRecordProcessors;
import io.taskite.streamplatform.api.record.TypedEventRegistry;
import io.taskite.streamplatform.api.state.KeyGenerator;
import io.taskite.streamplatform.api.writer.Writers;
import io.taskite.streamplatform.api.writer.WritersFactory;
import java.time.Duration;
import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

public final class PlacementDriverProcessors {
  private static final String MISSING_CONTEXT =
      "Expected to create PD processors, but processor context is null";
  private static final String MISSING_DEPENDENCIES =
      "Expected to create PD processors, but processor dependencies are null";

  private PlacementDriverProcessors() {}

  public static TypedEventRegistry createEventRegistry() {
    final TypedEventRegistry.Builder registry =
        TypedEventRegistry.builder()
            .register(
                ValueType.JOB_SUBSCRIPTION,
                JobSubscriptionIntent.CREATED,
                JobSubscriptionRecord::new)
            .register(
                ValueType.JOB_SUBSCRIPTION_DELETION,
                JobSubscriptionIntent.DELETED,
                JobSubscriptionDeletionRecord::new)
            .register(ValueType.TIMER, TimerSubscriptionRecord::new)
            .register(ValueType.BROKER_REGISTRATION, BrokerRegistrationRecord::new)
            .register(ValueType.WORKER_REGISTRATION, WorkerRegistrationRecord::new)
            .register(ValueType.JOB_YIELD, JobYieldRecord::new)
            .register(ValueType.PARTITION_CHANGE, PartitionChangeRecord::new)
            .register(
                ValueType.PARTITION_CHANGE,
                BrokerPartitionChangeIntent.EXECUTION_RESULT,
                PartitionExecutionRecord::new)
            .register(
                ValueType.PARTITION_CHANGE,
                BrokerPartitionChangeIntent.EXECUTION_UPDATED,
                PartitionExecutionRecord::new)
            .register(ValueType.MEMBER_CHANGE, MemberChangeRecord::new)
            .register(
                ValueType.MEMBER_CHANGE,
                MemberChangeIntent.EXECUTION_RESULT,
                MemberExecutionRecord::new)
            .register(
                ValueType.MEMBER_CHANGE,
                MemberChangeIntent.EXECUTION_UPDATED,
                MemberExecutionRecord::new)
            .register(ValueType.JOB_SUBSCRIPTION_MIRROR, JobSubscriptionMirrorRecord::new)
            .register(
                ValueType.JOB_SUBSCRIPTION_MIRROR,
                JobSubscriptionMirrorIntent.QUERY,
                JobSubscriptionMirrorQueryRecord::new)
            .register(
                ValueType.JOB_SUBSCRIPTION_MIRROR,
                JobSubscriptionMirrorIntent.QUERIED,
                JobSubscriptionMirrorQueryRecord::new)
            .register(ValueType.COMMAND_DISTRIBUTION, CommandDistributionRecord::new)
            .register(
                ValueType.COMMAND_DISTRIBUTION,
                CommandDistributionIntent.START,
                CommandDistributionRequestRecord::new)
            .register(
                ValueType.COMMAND_DISTRIBUTION,
                CommandDistributionIntent.QUERY,
                CommandDistributionQueryRecord::new)
            .register(
                ValueType.COMMAND_DISTRIBUTION,
                CommandDistributionIntent.QUERIED,
                CommandDistributionQueryRecord::new)
            .register(
                ValueType.COMMAND_DISTRIBUTION,
                CommandDistributionIntent.LIST,
                CommandDistributionListRecord::new)
            .register(
                ValueType.COMMAND_DISTRIBUTION,
                CommandDistributionIntent.LISTED,
                CommandDistributionListRecord::new)
            .register(
                ValueType.COMMAND_DISTRIBUTION,
                CommandDistributionIntent.REDISTRIBUTE,
                CommandDistributionRedistributeRecord::new);
    registerTopologyRecords(registry);
    return registry.build();
  }

  private static void registerTopologyRecords(final TypedEventRegistry.Builder registry) {
    registry
        .register(
            ValueType.TOPOLOGY, TopologyIntent.COORDINATE, BrokerTopologyCoordinationRecord::new)
        .register(
            ValueType.TOPOLOGY, TopologyIntent.COORDINATED, BrokerTopologyCoordinationRecord::new)
        .register(ValueType.TOPOLOGY, TopologyIntent.UPDATE, MemberTopologyRecord::new)
        .register(ValueType.TOPOLOGY, TopologyIntent.UPDATED, MemberTopologyRecord::new)
        .register(ValueType.TOPOLOGY, TopologyIntent.QUERY, TopologyQueryRecord::new)
        .register(ValueType.TOPOLOGY, TopologyIntent.QUERIED, TopologyQueryRecord::new)
        .register(ValueType.TOPOLOGY, TopologyIntent.CREATE_CLUSTER, ClusterConfigRecord::new)
        .register(ValueType.TOPOLOGY, TopologyIntent.CLUSTER_CREATED, ClusterConfigRecord::new)
        .register(ValueType.TOPOLOGY, TopologyIntent.CONFIGURE_CLUSTER, ClusterConfigRecord::new)
        .register(ValueType.TOPOLOGY, TopologyIntent.CLUSTER_CONFIGURED, ClusterConfigRecord::new)
        .register(ValueType.TOPOLOGY, TopologyIntent.DELETE_CLUSTER, ClusterConfigRecord::new)
        .register(ValueType.TOPOLOGY, TopologyIntent.CLUSTER_DELETED, ClusterConfigRecord::new)
        .register(
            ValueType.TOPOLOGY, TopologyIntent.CANCEL_CLUSTER_CHANGE, ClusterConfigRecord::new)
        .register(
            ValueType.TOPOLOGY, TopologyIntent.CLUSTER_CHANGE_CANCELLED, ClusterConfigRecord::new);
  }

  public static TypedRecordProcessors createPlacementDriverProcessors(
      final TypedRecordProcessorContext context,
      final PlacementDriverProcessorDependencies dependencies,
      final Supplier<BrokerClient> clients,
      final BooleanSupplier leader) {
    return createPlacementDriverProcessors(
        context, dependencies, clients, leader, Duration.ofSeconds(30));
  }

  public static TypedRecordProcessors createPlacementDriverProcessors(
      final TypedRecordProcessorContext context,
      final PlacementDriverProcessorDependencies dependencies,
      final Supplier<BrokerClient> clients,
      final BooleanSupplier leader,
      final Duration brokerObservationTimeout) {
    return createPlacementDriverProcessors(
        context, dependencies, clients, leader, brokerObservationTimeout, Duration.ofSeconds(5));
  }

  public static TypedRecordProcessors createPlacementDriverProcessors(
      final TypedRecordProcessorContext context,
      final PlacementDriverProcessorDependencies dependencies,
      final Supplier<BrokerClient> clients,
      final BooleanSupplier leader,
      final Duration brokerObservationTimeout,
      final Duration jobYieldRequestTimeout) {
    return createPlacementDriverProcessors(
        context,
        dependencies,
        clients,
        leader,
        brokerObservationTimeout,
        jobYieldRequestTimeout,
        () -> {
          throw new IllegalStateException("PD member execution requires native Raft transport");
        });
  }

  public static TypedRecordProcessors createPlacementDriverProcessors(
      final TypedRecordProcessorContext context,
      final PlacementDriverProcessorDependencies dependencies,
      final Supplier<BrokerClient> clients,
      final BooleanSupplier leader,
      final Duration brokerObservationTimeout,
      final Duration jobYieldRequestTimeout,
      final Supplier<PdMemberChangeTransport> pdTransports) {
    Objects.requireNonNull(context, MISSING_CONTEXT);
    Objects.requireNonNull(dependencies, MISSING_DEPENDENCIES);

    final MutableProcessingState state = context.getProcessingState(MutableProcessingState.class);
    final var timerChecker = new DueDateTimerChecker(state);
    final var timerTriggerProcessor =
        new TimerTriggerProcessor(context, timerChecker, clients, leader);
    final TypedRecordProcessors processors =
        TypedRecordProcessors.processors()
            .withLifecycleListener(dependencies.commandWriter())
            .withLifecycleListener(
                scheduling -> {
                  state.getCommandDistributionState().rebuildPendingIndex();
                  state.getBrokerTopologyChangeState().rebuildPendingIndex();
                  state.getBrokerPartitionTopologyState().rebuildTopologyIndex();
                })
            .withLifecycleListener(timerTriggerProcessor)
            .withLifecycleListener(timerChecker)
            .withLifecycleListener(
                new RegistrationDeadlineChecker(state, dependencies.registrationPolicy()));
    BrokerProcessors.registerBrokerProcessors(
        processors, context, dependencies.registrationPolicy().brokerLeaseTimeout());
    WorkerProcessors.register(
        processors,
        context,
        dependencies.registrationPolicy().workerLeaseTimeout(),
        dependencies.registrationPolicy().workerOfflineRetention());
    processors
        .onCommand(
            ValueType.TIMER, TimerIntent.CREATE, new TimerCreateProcessor(context, timerChecker))
        .onCommand(ValueType.TIMER, TimerIntent.CANCEL, new TimerCancelProcessor(context))
        .onCommand(ValueType.TIMER, TimerIntent.TRIGGER, timerTriggerProcessor)
        .onCommand(
            ValueType.JOB_YIELD,
            JobYieldIntent.UPDATE_PROGRESS,
            new JobYieldUpdateProgressProcessor(context))
        .onCommand(
            ValueType.JOB_YIELD,
            JobYieldIntent.ACKNOWLEDGE,
            new JobYieldAcknowledgeProcessor(context));
    TopologyQueryProcessors.registerTopologyQueryProcessors(processors, context);
    processors
        .withLifecycleListener(
            new BrokerTopologyCoordinatorChecker(
                state.getBrokerTopologyCoordinatorState(), leader, System::currentTimeMillis))
        .onCommand(
            ValueType.TOPOLOGY,
            TopologyIntent.COORDINATE,
            new TopologyCoordinateProcessor(
                context,
                leader,
                dependencies.clusterChangePlanningService(),
                Duration.ofMinutes(5)));
    {
      final var dispatchProcessor =
          new CommandDistributionDispatchProcessor(
              context,
              leader,
              () -> new GrpcBrokerJobSubscriptionTransport(clients.get()),
              new ImmutableStateBrokerDistributionRouteResolver(state),
              brokerObservationTimeout);
      processors
          .withLifecycleListener(dispatchProcessor)
          .withLifecycleListener(
              new BrokerDistributionScheduler(
                  state.getCommandDistributionState(), leader, System::currentTimeMillis))
          .onCommand(
              ValueType.COMMAND_DISTRIBUTION, CommandDistributionIntent.DISPATCH, dispatchProcessor)
          .onCommand(
              ValueType.COMMAND_DISTRIBUTION,
              CommandDistributionIntent.RETRY,
              new CommandDistributionRetryProcessor(context, dispatchProcessor::active));
    }
    {
      final var yieldSender =
          new JobYieldCommandSender(
              context,
              leader,
              new ImmutableStateBrokerDistributionRouteResolver(state),
              () -> new GrpcJobYieldTransport(jobYieldRequestTimeout),
              jobYieldRequestTimeout.plusSeconds(15));
      processors
          .withLifecycleListener(yieldSender)
          .withLifecycleListener(
              new JobYieldScheduler(state.getJobYieldState(), leader, System::currentTimeMillis))
          .onCommand(
              ValueType.JOB_YIELD,
              JobYieldIntent.DISPATCH,
              yieldSender.new JobYieldDispatchProcessor())
          .onCommand(
              ValueType.JOB_YIELD,
              JobYieldIntent.SYNC_PARTITION,
              yieldSender.new JobYieldSyncPartitionProcessor())
          .onCommand(
              ValueType.JOB_YIELD,
              JobYieldIntent.UPDATE_PARTITION_SYNC,
              yieldSender.new JobYieldUpdatePartitionSyncProcessor());
    }
    {
      final var execution =
          new TopologyMembershipChangeExecutor(
              context,
              leader,
              () -> new GrpcBrokerClusterChangeTransport(clients.get()),
              pdTransports,
              brokerObservationTimeout,
              Duration.ofSeconds(15));
      final var partitionProcessor = new PartitionExecuteProcessor(context, execution);
      final var memberProcessor = new MemberExecuteProcessor(context, execution);
      processors
          .withLifecycleListener(execution)
          .withLifecycleListener(
              new TopologyMembershipChangeChecker(state, leader, System::currentTimeMillis))
          .onCommand(
              ValueType.PARTITION_CHANGE, BrokerPartitionChangeIntent.EXECUTE, partitionProcessor)
          .onCommand(
              ValueType.PARTITION_CHANGE,
              BrokerPartitionChangeIntent.EXECUTION_RESULT,
              partitionProcessor.results())
          .onCommand(ValueType.MEMBER_CHANGE, MemberChangeIntent.EXECUTE, memberProcessor)
          .onCommand(
              ValueType.MEMBER_CHANGE,
              MemberChangeIntent.EXECUTION_RESULT,
              memberProcessor.results());
    }
    BrokerClusterProcessors.registerBrokerClusterProcessors(processors, context, dependencies);
    TopologyMembershipChangeProcessors.register(processors, context);
    CommandDistributionProcessors.registerCommandDistributionProcessors(processors, context);
    JobSubscriptionMirrorProcessors.register(processors, context);
    PlacementDriverClusterProcessors.registerPlacementDriverClusterProcessors(processors, context);
    MemberProcessors.register(processors, context, dependencies.memberChangePlanners());
    return processors;
  }

  public static TypedRecordProcessorContext createProcessorContext(
      final int partitionId,
      final ProcessingStateStore storage,
      final KeyGenerator keyGenerator,
      final WritersFactory writersFactory,
      final LongSupplier committedPositionSupplier,
      final LongSupplier termSupplier,
      final int changeHistoryLimit) {
    return createProcessorContext(
        partitionId,
        storage,
        keyGenerator,
        writersFactory,
        committedPositionSupplier,
        termSupplier,
        changeHistoryLimit,
        new RegistrationPolicy(
            PlacementDriverConfiguration.DEFAULT_BROKER_LEASE_TIMEOUT,
            PlacementDriverConfiguration.DEFAULT_WORKER_LEASE_TIMEOUT,
            PlacementDriverConfiguration.DEFAULT_WORKER_OFFLINE_RETENTION));
  }

  public static TypedRecordProcessorContext createProcessorContext(
      final int partitionId,
      final ProcessingStateStore storage,
      final KeyGenerator keyGenerator,
      final WritersFactory writersFactory,
      final LongSupplier committedPositionSupplier,
      final LongSupplier termSupplier,
      final int changeHistoryLimit,
      final RegistrationPolicy registrationPolicy) {
    final MutableProcessingState state =
        new MutableProcessingState(
            storage,
            committedPositionSupplier,
            termSupplier,
            changeHistoryLimit,
            registrationPolicy);
    final var eventAppliers = PlacementDriverEventAppliers.create(state);
    final Writers writers = writersFactory.create(eventAppliers);
    return new TypedRecordProcessorContextImpl(
        partitionId, keyGenerator, writers, state, eventAppliers);
  }
}
