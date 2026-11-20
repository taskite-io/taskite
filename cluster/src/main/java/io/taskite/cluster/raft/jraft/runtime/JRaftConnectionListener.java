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
package io.taskite.cluster.raft.jraft.runtime;

import com.alipay.sofa.jraft.ReplicatorGroup;
import com.alipay.sofa.jraft.core.ReplicatorGroupImpl;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.util.Endpoint;

/**
 * Adapts physical connection notifications to the group's current consensus identities.
 *
 * <p>JRaft 1.4.1 GrpcClient uses ReplicatorGroup only as a checkReplicator callback. This
 * uninitialized adapter owns no replicators or timers; all notifications go to the real group.
 */
final class JRaftConnectionListener extends ReplicatorGroupImpl {
  private final ReplicatorGroup group;
  private final JRaftEndpointDirectory directory;

  JRaftConnectionListener(final ReplicatorGroup group, final JRaftEndpointDirectory directory) {
    this.group = group;
    this.directory = directory;
  }

  @Override
  public void checkReplicator(final PeerId physical, final boolean lock) {
    for (final Endpoint identity : directory.identities(physical.getEndpoint())) {
      group.checkReplicator(new PeerId(identity, 0), lock);
    }
  }
}
