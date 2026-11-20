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
package io.taskite.placementdriver.registry;

public enum RegistrationDeadlineType {
  BROKER_LEASE(1),
  WORKER_LEASE(2),
  WORKER_OFFLINE_RETENTION(3);

  private final byte value;

  RegistrationDeadlineType(final int value) {
    this.value = (byte) value;
  }

  public byte value() {
    return value;
  }

  public static RegistrationDeadlineType fromValue(final byte value) {
    return switch (value) {
      case 1 -> BROKER_LEASE;
      case 2 -> WORKER_LEASE;
      case 3 -> WORKER_OFFLINE_RETENTION;
      default -> throw new IllegalArgumentException("Unknown registration deadline type " + value);
    };
  }
}
