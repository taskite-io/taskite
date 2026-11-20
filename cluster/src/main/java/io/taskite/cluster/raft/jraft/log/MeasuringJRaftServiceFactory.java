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
package io.taskite.cluster.raft.jraft.log;

import com.alipay.sofa.jraft.JRaftServiceFactory;
import com.alipay.sofa.jraft.core.DefaultJRaftServiceFactory;
import com.alipay.sofa.jraft.entity.codec.LogEntryCodecFactory;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.storage.LogStorage;
import com.alipay.sofa.jraft.storage.RaftMetaStorage;
import com.alipay.sofa.jraft.storage.SnapshotStorage;
import com.alipay.sofa.jraft.util.Endpoint;
import io.taskite.cluster.raft.jraft.runtime.JRaftSnapshotStorage;
import java.util.Objects;

/** Wraps only JRaft log storage and delegates every other service to the default factory. */
public final class MeasuringJRaftServiceFactory implements JRaftServiceFactory {
  private final JRaftServiceFactory delegate = DefaultJRaftServiceFactory.newInstance();
  private final JRaftSnapshotProgress progress;
  private final Endpoint snapshotEndpoint;

  public MeasuringJRaftServiceFactory(final JRaftSnapshotProgress progress) {
    this(progress, null);
  }

  public MeasuringJRaftServiceFactory(
      final JRaftSnapshotProgress progress, final Endpoint snapshotEndpoint) {
    this.progress = Objects.requireNonNull(progress, "progress");
    this.snapshotEndpoint = snapshotEndpoint;
  }

  @Override
  public LogStorage createLogStorage(final String uri, final RaftOptions raftOptions) {
    return new MeasuredLogStorage(delegate.createLogStorage(uri, raftOptions), progress);
  }

  @Override
  public SnapshotStorage createSnapshotStorage(final NodeOptions options) {
    return snapshotEndpoint == null
        ? delegate.createSnapshotStorage(options)
        : new JRaftSnapshotStorage(options, snapshotEndpoint);
  }

  @Override
  public RaftMetaStorage createRaftMetaStorage(final String uri, final RaftOptions raftOptions) {
    return delegate.createRaftMetaStorage(uri, raftOptions);
  }

  @Override
  public LogEntryCodecFactory createLogEntryCodecFactory() {
    return delegate.createLogEntryCodecFactory();
  }
}
