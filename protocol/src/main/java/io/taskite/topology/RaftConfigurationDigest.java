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
package io.taskite.topology;

import io.taskite.protocol.record.value.broker.BrokerPartitionMember;
import io.taskite.util.HostPort;
import io.taskite.util.RaftMemberIdentity;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.HexFormat;

/**
 * Canonical endpoint/identity/priority digest; independent of configuration list order and IPv6
 * endpoint brackets.
 */
public final class RaftConfigurationDigest {
  private RaftConfigurationDigest() {}

  public record Member(String brokerId, String host, int port, boolean voter, int priority) {}

  public static String ofBroker(
      final String clusterId, final Collection<BrokerPartitionMember> members) {
    return of(
        members.stream()
            .map(
                member -> {
                  final var identity =
                      HostPort.parse(RaftMemberIdentity.of(clusterId, member.brokerId()));
                  return new Member(
                      member.brokerId(),
                      identity.host(),
                      identity.port(),
                      member.voter(),
                      member.priority());
                })
            .toList());
  }

  public static String of(final Collection<Member> members) {
    try {
      final MessageDigest digest = MessageDigest.getInstance("SHA-256");
      members.stream()
          .map(
              member ->
                  part(member.brokerId())
                      + part(HostPort.normalizeHost(member.host()))
                      + member.port()
                      + ":"
                      + member.voter()
                      + ":"
                      + member.priority())
          .sorted()
          .forEach(value -> digest.update(part(value).getBytes(StandardCharsets.UTF_8)));
      return HexFormat.of().formatHex(digest.digest());
    } catch (final NoSuchAlgorithmException error) {
      throw new IllegalStateException(error);
    }
  }

  private static String part(final String value) {
    return value.length() + ":" + value;
  }
}
