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
package io.taskite.protocol.impl.encoding;

import io.taskite.protocol.record.*;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public final class ErrorResponse {

  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();

  private final ErrorResponseEncoder bodyEncoder = new ErrorResponseEncoder();
  private final ErrorResponseDecoder bodyDecoder = new ErrorResponseDecoder();
  private final DirectBuffer errorMessage = new UnsafeBuffer();
  private ErrorCode errorCode;

  public ErrorResponse() {
    reset();
  }

  public ErrorResponse reset() {
    errorCode = ErrorCode.NULL_VAL;
    errorMessage.wrap(0, 0);
    return this;
  }

  public ErrorCode getErrorCode() {
    return errorCode;
  }

  public ErrorResponse setErrorCode(final ErrorCode errorCode) {
    this.errorCode = errorCode;
    return this;
  }

  public DirectBuffer getErrorMessage() {
    return errorMessage;
  }

  public ErrorResponse setErrorMessage(final DirectBuffer errorMessage) {
    this.errorMessage.wrap(errorMessage, 0, errorMessage.capacity());
    return this;
  }

  public boolean tryWrap(final DirectBuffer buffer) {
    return tryWrap(buffer, 0, buffer.capacity());
  }

  public boolean tryWrap(final DirectBuffer buffer, final int offset, final int length) {
    headerDecoder.wrap(buffer, offset);

    return headerDecoder.schemaId() == bodyDecoder.sbeSchemaId()
        && headerDecoder.templateId() == bodyDecoder.sbeTemplateId();
  }

  public void wrap(final DirectBuffer buffer, int offset, final int length) {
    reset();

    final int frameEnd = offset + length;

    headerDecoder.wrap(buffer, offset);

    offset += headerDecoder.encodedLength();

    bodyDecoder.wrap(buffer, offset, headerDecoder.blockLength(), headerDecoder.version());

    errorCode = bodyDecoder.errorCode();

    offset += bodyDecoder.sbeBlockLength();

    final int errorDataLength = bodyDecoder.errorMessageLength();
    offset += ErrorResponseDecoder.errorMessageHeaderLength();

    if (errorDataLength > 0) {
      errorMessage.wrap(buffer, offset, errorDataLength);
      offset += errorDataLength;
    }

    bodyDecoder.limit(offset);

    assert bodyDecoder.limit() == frameEnd
        : "Decoder read only to position "
            + bodyDecoder.limit()
            + " but expected "
            + frameEnd
            + " as final position";
  }

  public int getLength() {
    return MessageHeaderEncoder.ENCODED_LENGTH
        + ErrorResponseEncoder.BLOCK_LENGTH
        + ErrorResponseEncoder.errorMessageHeaderLength()
        + errorMessage.capacity();
  }

  public void write(final MutableDirectBuffer buffer, int offset) {
    headerEncoder
        .wrap(buffer, offset)
        .blockLength(bodyEncoder.sbeBlockLength())
        .templateId(bodyEncoder.sbeTemplateId())
        .schemaId(bodyEncoder.sbeSchemaId())
        .version(bodyEncoder.sbeSchemaVersion());

    offset += headerEncoder.encodedLength();

    bodyEncoder
        .wrap(buffer, offset)
        .errorCode(errorCode)
        .putErrorMessage(errorMessage, 0, errorMessage.capacity());
  }

  @Override
  public String toString() {
    return "ErrorResponse{"
        + "errorCode="
        + errorCode
        + ", errorMessage='"
        + errorMessage
        + '\''
        + '}';
  }
}
