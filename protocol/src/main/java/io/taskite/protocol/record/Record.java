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
package io.taskite.protocol.record;

import io.taskite.protocol.record.intent.Intent;

/** Represents a record published to the log stream. */
public interface Record<T extends RecordValue> extends JsonSerializable {
  /**
   * Retrieves the position of the record. Positions are locally unique to the partition, and
   * monotonically increasing. Records are then ordered on the partition by their positions, i.e.
   * lower position means the record was published earlier.
   *
   * @return position the record
   */
  long getPosition();

  /**
   * Retrieves the key of the record.
   *
   * <p>Multiple records can have the same key if they belongs to the same logical entity. Keys are
   * unique for the combination of partition and record type.
   *
   * @return the key of the record
   */
  long getKey();

  /**
   * @return the unix timestamp at which the record was published on the partition.
   */
  long getTimestamp();

  /**
   * @return the intent of the record
   */
  Intent getIntent();

  /**
   * @return the partition ID on which the record was published
   */
  int getPartitionId();

  /**
   * @return the type of the record (event, command or command rejection)
   */
  RecordType getRecordType();

  /**
   * @return the type of rejection if {@link #getRecordType()} returns {@link
   *     RecordType#COMMAND_REJECTION} or else <code>null</code>.
   */
  RejectionType getRejectionType();

  /**
   * @return the reason why a command was rejected if {@link #getRecordType()} returns {@link
   *     RecordType#COMMAND_REJECTION} or else <code>null</code>.
   */
  String getRejectionReason();

  /**
   * A record version is an integer starting from 1. The version of a record is defined when it is
   * written. It allows different versions of the same record to be processed or applied
   * differently.
   *
   * <p>For example, it allows us to apply an older event in the same way as when it was originally
   * written, while allowing newer events to be applied differently.
   *
   * @return the version of the record when written
   */
  int getRecordVersion();

  /**
   * @return the type of the record (e.g. job, process, process instance, etc.)
   */
  ValueType getValueType();

  /**
   * Returns the raw value of the record, which should implement one of the interfaces in the {@link
   * io.taskite.protocol.record.value} package.
   *
   * <p>The record value is essentially the record specific data, e.g. for a process instance
   * creation event, it would contain information relevant to the process instance being created.
   *
   * @return record value
   */
  T getValue();
}
