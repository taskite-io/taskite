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

import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.option.SnapshotCopierOptions;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotCopier;
import com.alipay.sofa.jraft.storage.snapshot.local.LocalSnapshotStorage;
import com.alipay.sofa.jraft.util.Endpoint;
import io.taskite.util.HostPort;

/**
 * Advertises the physical file server and adapts its URI through public snapshot/client APIs.
 * JRaft's built-in copier splits host:port on every colon and cannot parse IPv6. Its parser
 * receives an opaque alias here; a dedicated file client dials the exact physical endpoint from the
 * URI.
 */
public final class JRaftSnapshotStorage extends LocalSnapshotStorage {
  private final NodeOptions options;
  private final JRaftSnapshotFileClient files;
  private boolean fileClientStarted;

  public JRaftSnapshotStorage(final NodeOptions options, final Endpoint endpoint) {
    super(options.getSnapshotUri(), options.getSnapshotTempUri(), options.getRaftOptions());
    this.options = options;
    files = new JRaftSnapshotFileClient(options);
    setServerAddr(endpoint);
  }

  @Override
  public synchronized SnapshotCopier startToCopyFrom(
      final String uri, final SnapshotCopierOptions source) {
    if (uri == null || !uri.startsWith("remote://")) {
      throw new IllegalArgumentException("Invalid remote snapshot URI: " + uri);
    }
    final int slash = uri.indexOf('/', "remote://".length());
    if (slash < 0) {
      throw new IllegalArgumentException("Snapshot URI lacks reader ID: " + uri);
    }
    final var address = HostPort.parse(uri.substring("remote://".length(), slash));
    final var alias = files.register(address);
    if (!fileClientStarted) {
      if (!JRaftLogicalRpcFactory.withoutGroup(() -> files.init(options))) {
        throw new IllegalStateException("Failed to initialize snapshot file client");
      }
      fileClientStarted = true;
    }
    final var copy =
        new SnapshotCopierOptions(
            source.getGroupId(),
            files,
            source.getTimerManager(),
            source.getRaftOptions(),
            source.getNodeOptions());
    return super.startToCopyFrom("remote://" + alias + uri.substring(slash), copy);
  }

  @Override
  public synchronized void shutdown() {
    super.shutdown();
    if (fileClientStarted) {
      files.shutdown();
      fileClientStarted = false;
    }
    files.clearDestinations();
  }
}
