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

public final class BrokerErrorResponse<T> extends BrokerResponse<T> {

  private final BrokerError error;

  public BrokerErrorResponse(final BrokerError error) {
    super();
    this.error = error;
  }

  @Override
  public boolean isError() {
    return true;
  }

  @Override
  public BrokerError getError() {
    return error;
  }

  @Override
  public String toString() {
    return "BrokerErrorResponse{" + "error=" + error + '}';
  }
}
