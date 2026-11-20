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

import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.util.BufferUtil;
import org.agrona.DirectBuffer;

/**
 * Represents a command rejection from the broker.
 *
 * @param intent the intent of the command that was rejected
 * @param key the key of the command that was rejected
 * @param type the type of the rejection
 * @param reason the reason for the rejection
 */
public record BrokerRejection(Intent intent, long key, RejectionType type, String reason) {

  public BrokerRejection(Intent intent, long key, RejectionType type, DirectBuffer reason) {
    // The rejection reason is a UTF-8 payload, not an object: decode the buffer's bytes rather than
    // calling toString() on it (which would yield "UnsafeBuffer{addressOffset=...}").
    this(intent, key, type, BufferUtil.bufferAsString(reason));
  }
}
