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
package io.taskite.broker.client.response;

public class BrokerResponse<T> {

  private final boolean isResponse;
  private final T response;
  private final int partitionId;
  private final long key;

  protected BrokerResponse() {
    isResponse = false;
    response = null;
    partitionId = -1;
    key = -1;
  }

  public BrokerResponse(final T response) {
    this(response, -1, -1);
  }

  public BrokerResponse(final T response, final int partitionId, final long key) {
    isResponse = true;
    this.response = response;
    this.partitionId = partitionId;
    this.key = key;
  }

  public boolean isError() {
    return false;
  }

  public BrokerError getError() {
    return null;
  }

  public boolean isRejection() {
    return false;
  }

  public BrokerRejection getRejection() {
    return null;
  }

  public boolean isResponse() {
    return isResponse;
  }

  public T getResponse() {
    return response;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public long getKey() {
    return key;
  }

  @Override
  public String toString() {
    return "BrokerResponse{"
        + "isResponse="
        + isResponse
        + ", response="
        + response
        + ", partitionId="
        + partitionId
        + ", key="
        + key
        + '}';
  }
}
