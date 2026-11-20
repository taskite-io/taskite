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
package io.taskite.placementdriver.runtime;

import io.taskite.protocol.record.RejectionType;

/** Preserves a command rejection across local and forwarded placement-driver submissions. */
public final class PlacementDriverRejectionException extends RuntimeException {
  private final RejectionType type;
  private final String reason;

  public PlacementDriverRejectionException(final RejectionType type, final String reason) {
    super(reason == null || reason.isBlank() ? String.valueOf(type) : reason);
    this.type = type == null ? RejectionType.NULL_VAL : type;
    this.reason = reason == null ? "" : reason;
  }

  public RejectionType type() {
    return type;
  }

  public String reason() {
    return reason;
  }
}
