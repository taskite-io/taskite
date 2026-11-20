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

import io.taskite.protocol.impl.encoding.ErrorResponse;
import io.taskite.protocol.record.ErrorCode;

public record BrokerError(ErrorCode code, String message) {

  public BrokerError(final ErrorResponse errorResponse) {
    // TODO: handle DirectBuffer to String
    this(errorResponse.getErrorCode(), errorResponse.getErrorMessage().toString());
  }

  public ErrorCode getCode() {
    return code;
  }

  public String getMessage() {
    return message;
  }

  @Override
  public String toString() {
    return "BrokerError{" + "code=" + code + ", message='" + message + '\'' + '}';
  }
}
