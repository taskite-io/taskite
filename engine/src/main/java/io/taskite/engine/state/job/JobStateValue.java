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
package io.taskite.engine.state.job;

import io.taskite.db.DbValue;
import io.taskite.engine.state.immutable.JobState.State;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * A {@link DbValue} holding a job's lifecycle {@link State}, encoded as the enum ordinal in one
 * byte.
 */
public final class JobStateValue implements DbValue {

  private State state = State.NOT_FOUND;

  @Override
  public int getLength() {
    return Byte.BYTES;
  }

  @Override
  public void write(final MutableDirectBuffer buffer, final int offset) {
    buffer.putByte(offset, (byte) state.ordinal());
  }

  @Override
  public void wrap(final DirectBuffer buffer, final int offset, final int length) {
    state = State.values()[buffer.getByte(offset)];
  }

  public State getState() {
    return state;
  }

  public void setState(final State state) {
    this.state = state;
  }

  @Override
  public String toString() {
    return "JobStateValue{" + state + '}';
  }
}
