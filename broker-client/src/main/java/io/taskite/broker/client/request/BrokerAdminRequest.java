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

import io.taskite.broker.client.response.*;
import io.taskite.protocol.impl.encoding.ErrorResponse;
import io.taskite.protocol.impl.encoding.ExecuteAdminCommand;
import io.taskite.protocol.record.*;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.util.BufferUtil;
import org.agrona.DirectBuffer;

public abstract class BrokerAdminRequest<T extends UnifiedRecordValue>
    extends ExecuteAdminCommand<T> implements BrokerRequest<T> {
  protected BrokerAdminRequest(final ValueType valueType, final Intent intent) {
    super(valueType, intent);
  }

  @Override
  public final boolean addressesSpecificPartition() {
    return true;
  }

  @Override
  public final boolean allowsTransportRetry() {
    return false;
  }

  @Override
  public final BrokerResponse<T> getResponse(final DirectBuffer buffer) {
    final var header = new MessageHeaderDecoder().wrap(buffer, 0);
    if (header.schemaId() == ErrorResponseDecoder.SCHEMA_ID
        && header.templateId() == ErrorResponseDecoder.TEMPLATE_ID) {
      final var error = new ErrorResponse();
      error.wrap(buffer, 0, buffer.capacity());
      return new BrokerErrorResponse<>(
          new BrokerError(
              error.getErrorCode(), BufferUtil.bufferAsString(error.getErrorMessage())));
    }
    response.wrap(buffer, 0, buffer.capacity());
    if (response.getValueType() != getValueType()
        || response.getPartitionId() != getPartitionId()
        || !response.getClusterId().equals(getClusterId())
        || response.getIntent().value()
            != getIntent().value() + (response.getRecordType() == RecordType.EVENT ? 1 : 0)) {
      throw new IllegalArgumentException("administration response identity or intent mismatch");
    }
    if (response.getRecordType() == RecordType.COMMAND_REJECTION) {
      return new BrokerRejectionResponse<>(
          new BrokerRejection(
              getIntent(), -1, response.getRejectionType(), response.getRejectionReason()));
    }
    return new BrokerResponse<>(toResponseDto(response.getValue()), getPartitionId(), -1);
  }

  @Override
  public final BrokerResponse<T> mapResponse(
      final int partitionId, final long key, final UnifiedRecordValue value) {
    throw new UnsupportedOperationException("administration responses are not engine records");
  }
}
