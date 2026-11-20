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
package io.taskite.protocol.impl.record.value.placementdriver.topology;

import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.value.placementdriver.topology.TopologyQueryRecordValue;

public class TopologyQueryRecord extends UnifiedRecordValue implements TopologyQueryRecordValue {
  private QueryType queryType = QueryType.BROKER_TOPOLOGY;
  private QueryScope scope = QueryScope.BROKER;
  private QueryView view = QueryView.TOPOLOGY;
  private ChangeKind changeKind = ChangeKind.AGGREGATE;
  private String clusterId = "";
  private String changeId = "";
  private String requestingMemberId = "";
  private long requestedAtEpochMillis;
  private String requestingHost = "";
  private int requestingHttpPort;
  private int requestingGrpcPort;
  private long requestingCommitIndex;
  private long requestingLastApplied;
  private long requestingLastLogIndex;
  private String responseValue = "";

  @Override
  public QueryType getQueryType() {
    return queryType;
  }

  public TopologyQueryRecord setQueryType(final QueryType queryType) {
    this.queryType = queryType == null ? QueryType.BROKER_TOPOLOGY : queryType;
    scope = this.queryType.scope();
    view = this.queryType.view();
    changeKind = this.queryType.changeKind();
    return this;
  }

  @Override
  public QueryScope getScope() {
    return scope;
  }

  public TopologyQueryRecord setScope(final QueryScope scope) {
    this.scope = scope == null ? QueryScope.BROKER : scope;
    return this;
  }

  @Override
  public QueryView getView() {
    return view;
  }

  public TopologyQueryRecord setView(final QueryView view) {
    this.view = view == null ? QueryView.TOPOLOGY : view;
    return this;
  }

  @Override
  public ChangeKind getChangeKind() {
    return changeKind;
  }

  public TopologyQueryRecord setChangeKind(final ChangeKind changeKind) {
    this.changeKind = changeKind == null ? ChangeKind.AGGREGATE : changeKind;
    return this;
  }

  public boolean hasConsistentSelector() {
    return queryType.matches(scope, view, changeKind);
  }

  @Override
  public String getClusterId() {
    return clusterId;
  }

  public TopologyQueryRecord setClusterId(final String clusterId) {
    this.clusterId = clusterId == null ? "" : clusterId;
    return this;
  }

  @Override
  public String getChangeId() {
    return changeId;
  }

  public TopologyQueryRecord setChangeId(final String changeId) {
    this.changeId = changeId == null ? "" : changeId;
    return this;
  }

  @Override
  public String getRequestingMemberId() {
    return requestingMemberId;
  }

  public TopologyQueryRecord setRequestingMemberId(final String requestingMemberId) {
    this.requestingMemberId = requestingMemberId == null ? "" : requestingMemberId;
    return this;
  }

  @Override
  public long getRequestedAtEpochMillis() {
    return requestedAtEpochMillis;
  }

  public TopologyQueryRecord setRequestedAtEpochMillis(final long requestedAtEpochMillis) {
    this.requestedAtEpochMillis = Math.max(0L, requestedAtEpochMillis);
    return this;
  }

  @Override
  public String getRequestingHost() {
    return requestingHost;
  }

  public TopologyQueryRecord setRequestingHost(final String requestingHost) {
    this.requestingHost = requestingHost == null ? "" : requestingHost;
    return this;
  }

  @Override
  public int getRequestingHttpPort() {
    return requestingHttpPort;
  }

  public TopologyQueryRecord setRequestingHttpPort(final int requestingHttpPort) {
    this.requestingHttpPort = requestingHttpPort;
    return this;
  }

  @Override
  public int getRequestingGrpcPort() {
    return requestingGrpcPort;
  }

  public TopologyQueryRecord setRequestingGrpcPort(final int requestingGrpcPort) {
    this.requestingGrpcPort = requestingGrpcPort;
    return this;
  }

  @Override
  public long getRequestingCommitIndex() {
    return requestingCommitIndex;
  }

  public TopologyQueryRecord setRequestingCommitIndex(final long requestingCommitIndex) {
    this.requestingCommitIndex = Math.max(0L, requestingCommitIndex);
    return this;
  }

  @Override
  public long getRequestingLastApplied() {
    return requestingLastApplied;
  }

  public TopologyQueryRecord setRequestingLastApplied(final long requestingLastApplied) {
    this.requestingLastApplied = Math.max(0L, requestingLastApplied);
    return this;
  }

  @Override
  public long getRequestingLastLogIndex() {
    return requestingLastLogIndex;
  }

  public TopologyQueryRecord setRequestingLastLogIndex(final long requestingLastLogIndex) {
    this.requestingLastLogIndex = Math.max(0L, requestingLastLogIndex);
    return this;
  }

  @Override
  public String getResponseValue() {
    return responseValue;
  }

  public TopologyQueryRecord setResponseValue(final String responseValue) {
    this.responseValue = responseValue == null ? "" : responseValue;
    return this;
  }
}
