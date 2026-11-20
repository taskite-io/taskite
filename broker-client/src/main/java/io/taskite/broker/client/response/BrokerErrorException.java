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

/** A broker-side error response returned for a client request. */
public class BrokerErrorException extends BrokerClientException {
  private static final String ERROR_MESSAGE_FORMAT = "Broker returned error (%s): %s";

  private final BrokerError error;

  public BrokerErrorException(final BrokerError error) {
    this(error, null);
  }

  public BrokerErrorException(final BrokerError error, final Throwable cause) {
    super(String.format(ERROR_MESSAGE_FORMAT, error.code().name(), error.message()), cause);
    this.error = error;
  }

  public BrokerError getError() {
    return error;
  }
}
