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
package io.taskite.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

/** Immutable Raft identity in PeerId endpoint syntax; never a transport destination. */
public final class RaftMemberIdentity {
  private RaftMemberIdentity() {}

  public static String of(final String clusterId, final String brokerId) {
    Objects.requireNonNull(clusterId, "clusterId");
    Objects.requireNonNull(brokerId, "brokerId");
    if (clusterId.isBlank() || brokerId.isBlank()) {
      throw new IllegalArgumentException("clusterId and brokerId must not be blank");
    }
    try {
      final String input =
          clusterId.length() + ":" + clusterId + brokerId.length() + ":" + brokerId;
      final String hash =
          HexFormat.of()
              .formatHex(
                  MessageDigest.getInstance("SHA-256")
                      .digest(input.getBytes(StandardCharsets.UTF_8)));
      return "b-" + hash.substring(0, 32) + "." + hash.substring(32) + ".raft.taskite.invalid:1";
    } catch (final NoSuchAlgorithmException error) {
      throw new IllegalStateException(error);
    }
  }
}
