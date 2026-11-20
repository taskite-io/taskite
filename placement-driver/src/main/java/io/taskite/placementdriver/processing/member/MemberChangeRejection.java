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
package io.taskite.placementdriver.processing.member;

import io.taskite.protocol.record.RejectionType;
import java.util.Objects;

public final class MemberChangeRejection extends RuntimeException {
  private static final String MISSING_REJECTION_TYPE =
      "Expected to reject a member change, but rejectionType is null";
  private final RejectionType rejectionType;

  public MemberChangeRejection(final RejectionType rejectionType, final String rejectionReason) {
    super(rejectionReason);
    this.rejectionType = Objects.requireNonNull(rejectionType, MISSING_REJECTION_TYPE);
  }

  public RejectionType rejectionType() {
    return rejectionType;
  }
}
