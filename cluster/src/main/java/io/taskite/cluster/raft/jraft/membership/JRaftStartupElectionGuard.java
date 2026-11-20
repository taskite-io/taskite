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
package io.taskite.cluster.raft.jraft.membership;

import com.alipay.sofa.jraft.conf.ConfigurationManager;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.codec.v2.LogEntryV2CodecFactory;
import com.alipay.sofa.jraft.option.LogStorageOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.storage.impl.RocksDBLogStorage;
import io.taskite.cluster.raft.storage.RaftStorageLayout;

/** Rejects the uncertified singleton fast election before NodeImpl.init can cast a vote. */
public final class JRaftStartupElectionGuard {
  private JRaftStartupElectionGuard() {}

  public static void verify(
      final String groupId, final PeerId self, final RaftStorageLayout layout, final int priority) {
    if (priority != 0) {
      return;
    }
    final var configurations = new ConfigurationManager();
    final var options = new LogStorageOptions();
    options.setGroupId(groupId);
    options.setConfigurationManager(configurations);
    options.setLogEntryCodecFactory(LogEntryV2CodecFactory.getInstance());
    final var logs = new RocksDBLogStorage(layout.logDir().toString(), new RaftOptions());
    try {
      if (!logs.init(options)) {
        throw new IllegalStateException(
            "Cannot inspect startup election configuration: " + groupId);
      }
      final var tail = configurations.getLastConfiguration();
      if (tail.isStable() && tail.getConf().size() == 1 && tail.getConf().contains(self)) {
        throw new IllegalStateException(
            "UNCERTIFIED_SINGLETON_ELECTION: "
                + groupId
                + "; committed recovery authorization required");
      }
    } finally {
      logs.shutdown();
    }
  }
}
