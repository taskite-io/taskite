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

/** A client command was rejected by the broker. */
public class BrokerRejectionException extends BrokerClientException {
  private static final String ERROR_MESSAGE_FORMAT = "Command (%s) rejected (%s): %s";

  private final BrokerRejection rejection;

  public BrokerRejectionException(final BrokerRejection rejection) {
    this(rejection, null);
  }

  public BrokerRejectionException(final BrokerRejection rejection, final Throwable cause) {
    super(
        String.format(
            ERROR_MESSAGE_FORMAT,
            rejection.intent().name(),
            rejection.type().name(),
            rejection.reason()),
        cause);
    this.rejection = rejection;
  }

  public BrokerRejection getRejection() {
    return rejection;
  }
}
