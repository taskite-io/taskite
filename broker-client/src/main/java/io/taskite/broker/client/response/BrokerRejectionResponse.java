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

public final class BrokerRejectionResponse<T> extends BrokerResponse<T> {

  private final BrokerRejection rejection;

  public BrokerRejectionResponse(final BrokerRejection rejection) {
    super();
    this.rejection = rejection;
  }

  @Override
  public boolean isRejection() {
    return true;
  }

  @Override
  public BrokerRejection getRejection() {
    return rejection;
  }

  @Override
  public String toString() {
    return "BrokerRejectionResponse{" + "rejection=" + rejection + '}';
  }
}
