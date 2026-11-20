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
package io.taskite.cluster.raft;

/**
 * A single member's identity and role within a raft group, derived from the committed raft
 * configuration. Engine-agnostic (surfaced by {@link RaftPartition#members()}) so the
 * placement-driver's {@code /members} endpoint can report the raft-consistent membership every node
 * agrees on.
 *
 * <p>Only membership facts that raft keeps strongly consistent are exposed: the member id and its
 * configured role. Per-node liveness is intentionally absent — it is a local, non-authoritative
 * observation (only the leader can vouch for peer reachability) and reporting it made otherwise
 * identical member lists look inconsistent across nodes without feeding any decision.
 *
 * @param memberId the business member id (broker/PD node id)
 * @param role this member's raft role from the current configuration
 */
public record RaftMember(String memberId, RaftRole role) {}
