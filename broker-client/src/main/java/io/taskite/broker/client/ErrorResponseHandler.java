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
package io.taskite.broker.client;

import io.taskite.protocol.record.ErrorCode;
import io.taskite.protocol.record.ErrorResponseDecoder;
import io.taskite.protocol.record.MessageHeaderDecoder;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public final class ErrorResponseHandler {
  private final ErrorResponseDecoder decoder = new ErrorResponseDecoder();

  private DirectBuffer errorMessage;

  public boolean handlesResponse(final MessageHeaderDecoder responseHeader) {
    return ErrorResponseDecoder.SCHEMA_ID == responseHeader.schemaId()
        && ErrorResponseDecoder.TEMPLATE_ID == responseHeader.templateId();
  }

  public void wrap(final DirectBuffer body, final int offset, final int length, final int version) {
    decoder.wrap(body, offset, length, version);

    final int errorDataLength = decoder.errorMessageLength();
    final byte[] errorData = new byte[errorDataLength];
    decoder.getErrorMessage(errorData, 0, errorDataLength);
    errorMessage = new UnsafeBuffer(errorData);
  }

  public ErrorCode getErrorCode() {
    return decoder.errorCode();
  }

  public DirectBuffer getErrorMessage() {
    return errorMessage;
  }
}
