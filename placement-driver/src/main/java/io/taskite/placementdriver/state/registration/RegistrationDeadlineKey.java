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
package io.taskite.placementdriver.state.registration;

import io.taskite.db.DbKey;
import io.taskite.db.impl.DbByte;
import io.taskite.db.impl.DbLong;
import io.taskite.db.impl.DbString;
import io.taskite.placementdriver.registry.RegistrationDeadline;
import io.taskite.placementdriver.registry.RegistrationDeadlineType;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

final class RegistrationDeadlineKey implements DbKey {
  private final DbLong dueDate = new DbLong();
  private final DbByte type = new DbByte();
  private final DbString clusterId = new DbString();
  private final DbString tenantId = new DbString();
  private final DbString resourceId = new DbString();

  RegistrationDeadlineKey wrapDeadline(final RegistrationDeadline deadline) {
    dueDate.wrapLong(deadline.dueDate());
    type.wrapByte(deadline.type().value());
    clusterId.wrapString(deadline.clusterId());
    tenantId.wrapString(deadline.tenantId());
    resourceId.wrapString(deadline.resourceId());
    return this;
  }

  long dueDate() {
    return dueDate.getValue();
  }

  RegistrationDeadline deadline() {
    return new RegistrationDeadline(
        dueDate.getValue(),
        RegistrationDeadlineType.fromValue(type.getValue()),
        clusterId.toString(),
        tenantId.toString(),
        resourceId.toString());
  }

  @Override
  public void wrap(final DirectBuffer buffer, final int offset, final int length) {
    int position = offset;
    dueDate.wrap(buffer, position, length);
    position += dueDate.getLength();
    type.wrap(buffer, position, length - (position - offset));
    position += type.getLength();
    clusterId.wrap(buffer, position, length - (position - offset));
    position += clusterId.getLength();
    tenantId.wrap(buffer, position, length - (position - offset));
    position += tenantId.getLength();
    resourceId.wrap(buffer, position, length - (position - offset));
  }

  @Override
  public int getLength() {
    return dueDate.getLength()
        + type.getLength()
        + clusterId.getLength()
        + tenantId.getLength()
        + resourceId.getLength();
  }

  @Override
  public void write(final MutableDirectBuffer buffer, final int offset) {
    int position = offset;
    dueDate.write(buffer, position);
    position += dueDate.getLength();
    type.write(buffer, position);
    position += type.getLength();
    clusterId.write(buffer, position);
    position += clusterId.getLength();
    tenantId.write(buffer, position);
    position += tenantId.getLength();
    resourceId.write(buffer, position);
  }
}
