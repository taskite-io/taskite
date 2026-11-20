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
package io.taskite.protocol.record.value.placementdriver.topology;

import com.fasterxml.jackson.annotation.JsonCreator;
import io.taskite.protocol.record.RecordValue;

public interface TopologyQueryRecordValue extends RecordValue {
  QueryType getQueryType();

  QueryScope getScope();

  QueryView getView();

  ChangeKind getChangeKind();

  String getClusterId();

  String getChangeId();

  String getRequestingMemberId();

  long getRequestedAtEpochMillis();

  String getRequestingHost();

  int getRequestingHttpPort();

  int getRequestingGrpcPort();

  long getRequestingCommitIndex();

  long getRequestingLastApplied();

  long getRequestingLastLogIndex();

  String getResponseValue();

  enum QueryType {
    BROKER_CLUSTER_CONFIGURATION(QueryScope.BROKER, QueryView.CONFIGURATION, ChangeKind.AGGREGATE),
    BROKER_CLUSTERS(QueryScope.BROKER, QueryView.CLUSTERS, ChangeKind.AGGREGATE),
    BROKER_TOPOLOGY(QueryScope.BROKER, QueryView.TOPOLOGY, ChangeKind.AGGREGATE),
    PLACEMENT_DRIVER_TOPOLOGY(QueryScope.PLACEMENT_DRIVER, QueryView.TOPOLOGY, ChangeKind.MEMBER),
    PLACEMENT_DRIVER_HEALTH(QueryScope.PLACEMENT_DRIVER, QueryView.HEALTH, ChangeKind.MEMBER),
    PLACEMENT_DRIVER_CONFIGURATION(
        QueryScope.PLACEMENT_DRIVER, QueryView.CONFIGURATION, ChangeKind.MEMBER),
    PLACEMENT_DRIVER_CHANGE(QueryScope.PLACEMENT_DRIVER, QueryView.CHANGE, ChangeKind.MEMBER),
    BROKER_CLUSTER_CHANGE(QueryScope.BROKER, QueryView.CHANGE, ChangeKind.AGGREGATE),
    TIMER_SUBSCRIPTIONS(QueryScope.BROKER, QueryView.TIMER_SUBSCRIPTIONS, ChangeKind.AGGREGATE);

    private final QueryScope scope;
    private final QueryView view;
    private final ChangeKind changeKind;

    QueryType(final QueryScope scope, final QueryView view, final ChangeKind changeKind) {
      this.scope = scope;
      this.view = view;
      this.changeKind = changeKind;
    }

    public QueryScope scope() {
      return scope;
    }

    public QueryView view() {
      return view;
    }

    public ChangeKind changeKind() {
      return changeKind;
    }

    public boolean matches(
        final QueryScope requestedScope,
        final QueryView requestedView,
        final ChangeKind requestedChangeKind) {
      return scope == requestedScope
          && view == requestedView
          && (view != QueryView.CHANGE || changeKind == requestedChangeKind);
    }

    @JsonCreator
    public static QueryType fromJson(final String value) {
      if (value == null || value.isBlank()) {
        return BROKER_TOPOLOGY;
      }
      return switch (value) {
        case "CLUSTER_CONFIG", "BROKER_CLUSTER_CONFIGURATION" -> BROKER_CLUSTER_CONFIGURATION;
        case "BROKER_CLUSTERS" -> BROKER_CLUSTERS;
        case "TIMER_SUBSCRIPTIONS" -> TIMER_SUBSCRIPTIONS;
        case "BROKERS", "BROKER_TOPOLOGY" -> BROKER_TOPOLOGY;
        case "MEMBERS", "PLACEMENT_DRIVER_TOPOLOGY" -> PLACEMENT_DRIVER_TOPOLOGY;
        case "HEALTH", "PLACEMENT_DRIVER_HEALTH" -> PLACEMENT_DRIVER_HEALTH;
        case "PLACEMENT_DRIVER_CONFIGURATION" -> PLACEMENT_DRIVER_CONFIGURATION;
        case "PLACEMENT_DRIVER_CHANGE" -> PLACEMENT_DRIVER_CHANGE;
        case "BROKER_CLUSTER_CHANGE_BATCH", "BROKER_CLUSTER_CHANGES", "BROKER_CLUSTER_CHANGE" ->
            BROKER_CLUSTER_CHANGE;
        default -> throw new IllegalArgumentException("Unsupported topology query type: " + value);
      };
    }
  }

  enum QueryScope {
    BROKER,
    PLACEMENT_DRIVER;

    @JsonCreator
    public static QueryScope fromJson(final String value) {
      if (value == null || value.isBlank()) {
        return BROKER;
      }
      return valueOf(value);
    }
  }

  enum QueryView {
    HEALTH,
    TOPOLOGY,
    CLUSTERS,
    CONFIGURATION,
    CHANGE,
    TIMER_SUBSCRIPTIONS;

    @JsonCreator
    public static QueryView fromJson(final String value) {
      if (value == null || value.isBlank()) {
        return TOPOLOGY;
      }
      return valueOf(value);
    }
  }

  enum ChangeKind {
    MEMBER,
    PARTITION,
    AGGREGATE;

    @JsonCreator
    public static ChangeKind fromJson(final String value) {
      if (value == null || value.isBlank()) {
        return AGGREGATE;
      }
      return valueOf(value);
    }
  }
}
