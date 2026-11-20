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
package io.taskite.broker.client.request;

import io.taskite.broker.client.response.BrokerError;
import io.taskite.broker.client.response.BrokerErrorResponse;
import io.taskite.broker.client.response.BrokerRejection;
import io.taskite.broker.client.response.BrokerRejectionResponse;
import io.taskite.broker.client.response.BrokerResponse;
import io.taskite.broker.client.response.UnsupportedBrokerResponseException;
import io.taskite.protocol.impl.encoding.ErrorResponse;
import io.taskite.protocol.impl.encoding.ExecuteCommand;
import io.taskite.protocol.record.*;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.util.BufferWriter;
import java.nio.charset.StandardCharsets;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BrokerExecuteCommand<T extends UnifiedRecordValue> extends ExecuteCommand<T>
    implements BrokerRequest<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerExecuteCommand.class);
  private static final int RESPONSE_SCHEMA_ID = ExecuteCommandResponseDecoder.SCHEMA_ID;
  private static final int RESPONSE_TEMPLATE_ID = ExecuteCommandResponseDecoder.TEMPLATE_ID;

  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final ErrorResponse errorResponse = new ErrorResponse();
  private long routingEpoch;

  public BrokerExecuteCommand(final ValueType valueType, final Intent intent) {
    super(valueType, intent);
  }

  @Override
  public long getRoutingEpoch() {
    return routingEpoch;
  }

  @Override
  public void setRoutingEpoch(final long routingEpoch) {
    this.routingEpoch = Math.max(0L, routingEpoch);
  }

  @Override
  public BrokerResponse<T> getResponse(final DirectBuffer responseBuffer) {
    try {
      if (isValidResponse(responseBuffer)) {
        wrapResponse(responseBuffer);
        return readResponse();
      } else if (isErrorResponse(responseBuffer)) {
        wrapErrorResponse(responseBuffer);
        final var error = new BrokerError(errorResponse);
        return new BrokerErrorResponse<>(error);
      }
      throw new UnsupportedBrokerResponseException(
          headerDecoder.schemaId(),
          headerDecoder.templateId(),
          RESPONSE_SCHEMA_ID,
          RESPONSE_TEMPLATE_ID);
    } catch (final RuntimeException error) {
      LOGGER.error(
          "Failed to read response: {}{}{}",
          error.getMessage(),
          System.lineSeparator(),
          responseBuffer);
      throw error;
    }
  }

  private BrokerResponse<T> readResponse() {
    if (isRejection()) {
      final BrokerRejection brokerRejection =
          new BrokerRejection(
              getIntent(), getKey(), responseRejectionType(), responseRejectionReason());
      return new BrokerRejectionResponse<>(brokerRejection);
    }
    if (isValidResponse()) {
      final T responseDto = toResponseDto(responseValue());
      return new BrokerResponse<>(responseDto, responsePartitionId(), responseKey());
    }
    throw new UnsupportedBrokerResponseException(
        getValueType().name(), response.getValueType().name());
  }

  @Override
  public boolean addressesSpecificPartition() {
    return getPartitionId() != ExecuteCommandRequestEncoder.partitionIdNullValue();
  }

  public T getRecordValue() {
    return toRecordValue(getRequestWriter());
  }

  @Override
  public BrokerResponse<T> mapResponse(
      final int partitionId, final long key, final UnifiedRecordValue value) {
    if (value == null) {
      return new BrokerResponse<>(null, partitionId, key);
    }

    final byte[] bytes = value.toJson().getBytes(StandardCharsets.UTF_8);
    final DirectBuffer buffer = new UnsafeBuffer(bytes);
    return new BrokerResponse<>(toResponseDto(buffer), partitionId, key);
  }

  private void wrapResponseHeader(final DirectBuffer buffer) {
    headerDecoder.wrap(buffer, 0);
  }

  private boolean isErrorResponse(final DirectBuffer buffer) {
    wrapResponseHeader(buffer);
    return headerDecoder.schemaId() == ErrorResponseEncoder.SCHEMA_ID
        && headerDecoder.templateId() == ErrorResponseDecoder.TEMPLATE_ID;
  }

  private boolean isValidResponse(final DirectBuffer buffer) {
    wrapResponseHeader(buffer);
    return headerDecoder.schemaId() == RESPONSE_SCHEMA_ID
        && headerDecoder.templateId() == RESPONSE_TEMPLATE_ID;
  }

  private void wrapErrorResponse(final DirectBuffer buffer) {
    errorResponse.wrap(buffer, 0, buffer.capacity());
  }

  @SuppressWarnings("unchecked")
  private T toRecordValue(final BufferWriter writer) {
    if (writer instanceof UnifiedRecordValue value) {
      return (T) value;
    }

    throw new IllegalStateException(
        "Expected request writer to extend UnifiedRecordValue but got "
            + writer.getClass().getName());
  }
}
