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

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.core.TaskiteElectionLifecycle;
import com.alipay.sofa.jraft.entity.LeaderChangeContext;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.RaftOutter.SnapshotMeta;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import io.taskite.cluster.raft.RaftRole;
import io.taskite.cluster.raft.jraft.log.JRaftAppendClosure;
import io.taskite.cluster.raft.jraft.log.JRaftLogEntryCodec;
import io.taskite.cluster.raft.jraft.log.JRaftSnapshotProgress;
import io.taskite.cluster.raft.jraft.membership.JRaftExecutionAuthority;
import io.taskite.cluster.raft.jraft.membership.JRaftSnapshotElectionCertificate;
import io.taskite.cluster.raft.log.PersistedRaftRecord;
import io.taskite.cluster.raft.log.RaftLogListener;
import io.taskite.snapshot.SnapshotStateBackend;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The jraft {@link com.alipay.sofa.jraft.StateMachine} backing one partition (migration decisions
 * D1/D3/D4/D5). {@code onApply} is the single, role-agnostic apply path: it decodes each committed
 * entry's position span ({@link JRaftLogEntryCodec}), reconstructs the {@link PersistedRaftRecord},
 * advances the committed-position high-water, and notifies the business {@link RaftLogListener} —
 * the same "committed entry available to apply" contract the in-house {@code
 * RaftPartition.subscribe} provides.
 *
 * <p><b>Snapshots (D4/D5):</b> when a {@link SnapshotStateBackend} is supplied, {@code
 * onSnapshotSave} materializes a RocksDB checkpoint of the business state under jraft's snapshot
 * writer path and registers its files, and {@code onSnapshotLoad} imports it back — reusing the
 * existing checkpoint mechanism inside jraft's own {@code InstallSnapshot} flow (which replaces the
 * in-house chunked snapshot install). The {@code processedPosition} cursor lives in the business
 * column family, so it round-trips inside the checkpoint automatically.
 *
 * <p>The business apply wiring (expanding a batch into per-position records into the {@code
 * StreamProcessor}, atomic state+position commit, replay-dedup idempotency) is completed at the
 * {@code StreamPartition} cut-over; the {@link RaftLogListener} + {@code openLogReader} seams are
 * already in place for it.
 */
public final class JRaftStateMachine extends StateMachineAdapter {
  /** Subdirectory of jraft's snapshot writer path that holds the RocksDB checkpoint files. */
  public static final String CHECKPOINT_DIR = "rocksdb";

  private static final Logger LOG = LoggerFactory.getLogger(JRaftStateMachine.class);
  private static final String APPLICATION_POSITION_FILE = "application-position";

  private final String groupId;
  private final AtomicLong committedPosition = new AtomicLong(0);
  private final AtomicLong snapshotPosition = new AtomicLong(0);
  private final AtomicLong currentTerm = new AtomicLong(0);
  private final AtomicReference<RaftLogListener> logListener = new AtomicReference<>();
  private final JRaftStateListener stateListener;
  private final SnapshotStateBackend snapshotBackend;
  private final JRaftSnapshotProgress snapshotProgress;
  private volatile LongSupplier processedPositionSupplier = () -> Long.MAX_VALUE;
  private final AtomicLong lastSavedSnapshotIndex = new AtomicLong();
  private final AtomicLong lastSavedSnapshotTerm = new AtomicLong();
  private final AtomicLong lastSavedSnapshotSizeBytes = new AtomicLong();
  private final AtomicLong snapshotSaveGeneration = new AtomicLong();
  private volatile Runnable configurationListener = () -> {};
  private JRaftExecutionAuthority executionAuthority;
  private volatile Consumer<TaskiteElectionLifecycle.Observation> snapshotConfigurationListener =
      ignored -> {};
  private volatile Function<SnapshotMeta, TaskiteElectionLifecycle.Observation>
      snapshotCertificateSupplier = ignored -> null;

  public JRaftStateMachine(final String groupId, final JRaftStateListener stateListener) {
    this(groupId, stateListener, null, new JRaftSnapshotProgress());
  }

  public JRaftStateMachine(
      final String groupId,
      final JRaftStateListener stateListener,
      final SnapshotStateBackend snapshotBackend) {
    this(groupId, stateListener, snapshotBackend, new JRaftSnapshotProgress());
  }

  public JRaftStateMachine(
      final String groupId,
      final JRaftStateListener stateListener,
      final SnapshotStateBackend snapshotBackend,
      final JRaftSnapshotProgress snapshotProgress) {
    this.groupId = groupId;
    this.stateListener = stateListener;
    this.snapshotBackend = snapshotBackend;
    this.snapshotProgress = snapshotProgress;
  }

  public void setSnapshotConfigurationListener(
      final Consumer<TaskiteElectionLifecycle.Observation> listener) {
    snapshotConfigurationListener = Objects.requireNonNull(listener, "listener");
  }

  public void setSnapshotCertificateSupplier(
      final Function<SnapshotMeta, TaskiteElectionLifecycle.Observation> supplier) {
    snapshotCertificateSupplier = Objects.requireNonNull(supplier, "supplier");
  }

  public void setConfigurationListener(final Runnable listener) {
    configurationListener = Objects.requireNonNull(listener, "listener");
  }

  public void setExecutionAuthority(final JRaftExecutionAuthority authority) {
    executionAuthority = Objects.requireNonNull(authority);
  }

  /** Registers the business layer's committed-entry listener (the {@code subscribe} seam). */
  public void setLogListener(final RaftLogListener listener) {
    logListener.set(listener);
  }

  public long committedPosition() {
    return committedPosition.get();
  }

  public long snapshotPosition() {
    return snapshotPosition.get();
  }

  public long currentTerm() {
    return currentTerm.get();
  }

  public void setProcessedPositionSupplier(final LongSupplier processedPositionSupplier) {
    this.processedPositionSupplier =
        Objects.requireNonNull(processedPositionSupplier, "processedPositionSupplier");
  }

  @Override
  public void onApply(final Iterator it) {
    while (it.hasNext()) {
      // The leader-side closure that completes the append future. jraft does NOT auto-run the done
      // closure for DATA entries (only for conf/error entries): FSMCallerImpl.doApplyTasks
      // delegates
      // entirely to onApply, so it must be run here. It is run in a finally so that a throwing
      // listener can never strand the append future (which would hang the caller until timeout).
      final Closure done = it.done();
      Status status = Status.OK();
      try {
        final ByteBuffer data = it.getData();
        if (data != null && JRaftLogEntryCodec.isExecutionAuthority(data)) {
          if (executionAuthority == null) {
            throw new IllegalStateException("authority is not bound");
          }
          executionAuthority.apply(
              JRaftLogEntryCodec.authorityGeneration(data),
              JRaftLogEntryCodec.authorityOwner(data),
              it.getIndex());
        } else if (data != null && data.remaining() >= JRaftLogEntryCodec.HEADER_LENGTH) {
          final PersistedRaftRecord record =
              JRaftLogEntryCodec.decode(data, it.getTerm(), it.getIndex());
          // Advance the committed application-position high-water to the batch's highest position.
          committedPosition.accumulateAndGet(record.highestPosition(), Math::max);
          final RaftLogListener listener = logListener.get();
          if (listener != null) {
            listener.onCommitted(record);
          }
        }
        // Stamp the jraft-assigned coordinates so the append future resolves to a fully-populated
        // PersistedRaftRecord (façade parity with the in-house RaftPartition.append).
        if (done instanceof final JRaftAppendClosure appendClosure) {
          appendClosure.stamp(it.getTerm(), it.getIndex());
        }
      } catch (final RuntimeException e) {
        LOG.error("Group {} failed to apply committed entry {}", groupId, it.getIndex(), e);
        status = new Status(RaftError.ESTATEMACHINE, "apply failed: %s", e.getMessage());
      } finally {
        if (done != null) {
          done.run(status);
        }
      }
      it.next();
    }
  }

  /**
   * Saves a RocksDB checkpoint of the business state into jraft's snapshot writer path and
   * registers every checkpoint file (D4). The checkpoint goes into a {@code rocksdb/} subdirectory
   * because RocksDB requires its target not to pre-exist (jraft has already created the writer
   * path).
   */
  @Override
  public void onSnapshotSave(final SnapshotWriter writer, final Closure done) {
    if (snapshotBackend == null) {
      try {
        saveElectionCertificate(writer);
        saveExecutionAuthority(writer);
        done.run(Status.OK());
      } catch (final RuntimeException error) {
        done.run(
            new Status(
                RaftError.EIO, "failed to save election certificate: %s", error.getMessage()));
      }
      return;
    }
    final long processedPosition = processedPositionSupplier.getAsLong();
    final long committedApplicationPosition = committedPosition.get();
    if (processedPosition < committedApplicationPosition) {
      done.run(
          new Status(
              RaftError.EBUSY,
              "business state is behind committed position: processed=%d, committed=%d",
              processedPosition,
              committedApplicationPosition));
      return;
    }
    try {
      final Path snapshotRoot = Path.of(writer.getPath());
      final Path checkpointDir = snapshotRoot.resolve(CHECKPOINT_DIR);
      snapshotBackend.save(checkpointDir);
      // The source-command cursor in RocksDB differs from the last consumed event position.
      // Preserve the log high-water as well so a restored sequencer cannot reuse old positions.
      Files.writeString(
          snapshotRoot.resolve(APPLICATION_POSITION_FILE),
          Long.toString(committedApplicationPosition));
      saveElectionCertificate(writer);
      saveExecutionAuthority(writer);
      // Register every checkpoint file (relative to the writer path) so jraft replicates them to
      // lagging followers via InstallSnapshot. Nested names (rocksdb/<file>) are honoured — jraft's
      // copier recreates parent directories on the receiving side.
      for (final String relativeName : listSnapshotFiles(snapshotRoot)) {
        if (JRaftSnapshotElectionCertificate.FILE.equals(relativeName)
            || JRaftExecutionAuthority.FILE.equals(relativeName)) {
          continue;
        }
        if (!writer.addFile(relativeName)) {
          done.run(new Status(RaftError.EIO, "failed to register snapshot file: " + relativeName));
          return;
        }
      }
      final var meta = writer.getCurrentMeta();
      final long snapshotIndex = meta == null ? 0 : meta.getLastIncludedIndex();
      final long snapshotTerm = meta == null ? currentTerm.get() : meta.getLastIncludedTerm();
      final long snapshotSizeBytes = directorySize(checkpointDir);
      snapshotBackend.onSaveCompleted(checkpointDir, snapshotIndex, snapshotTerm);
      snapshotPosition.accumulateAndGet(committedApplicationPosition, Math::max);
      lastSavedSnapshotIndex.set(snapshotIndex);
      lastSavedSnapshotTerm.set(snapshotTerm);
      lastSavedSnapshotSizeBytes.set(snapshotSizeBytes);
      snapshotSaveGeneration.incrementAndGet();
      LOG.info("Group {} saved snapshot checkpoint under {}", groupId, checkpointDir);
      done.run(Status.OK());
    } catch (final Exception e) {
      LOG.error("Group {} failed to save snapshot", groupId, e);
      done.run(new Status(RaftError.EIO, "failed to save snapshot: %s", e.getMessage()));
    }
  }

  /**
   * Restores the business state from a jraft snapshot by importing the checkpoint files back into
   * RocksDB (D5). Returns false on failure so jraft treats the load as fatal, as required.
   */
  @Override
  public boolean onSnapshotLoad(final SnapshotReader reader) {
    if (snapshotBackend == null) {
      try {
        loadExecutionAuthority(reader);
        snapshotConfigurationListener.accept(
            JRaftSnapshotElectionCertificate.read(Path.of(reader.getPath()), reader.load()));
        return true;
      } catch (final RuntimeException error) {
        LOG.error("Group {} invalid snapshot election certificate", groupId, error);
        return false;
      }
    }
    try {
      final Path checkpointDir = Path.of(reader.getPath()).resolve(CHECKPOINT_DIR);
      if (!Files.isDirectory(checkpointDir)) {
        LOG.error("Group {} snapshot missing checkpoint dir {}", groupId, checkpointDir);
        return false;
      }
      snapshotBackend.beforeLoad().join();
      snapshotBackend.load(checkpointDir);
      snapshotPosition.set(0L);
      final Path applicationPosition = Path.of(reader.getPath()).resolve(APPLICATION_POSITION_FILE);
      if (Files.exists(applicationPosition)) {
        final long restoredPosition = Long.parseLong(Files.readString(applicationPosition).trim());
        if (restoredPosition < 0) {
          throw new IOException("Invalid snapshot application position: " + restoredPosition);
        }
        committedPosition.set(restoredPosition);
        snapshotPosition.set(restoredPosition);
      }
      final var meta = reader.load();
      final long snapshotIndex = meta == null ? 0 : meta.getLastIncludedIndex();
      final long snapshotTerm = meta == null ? currentTerm.get() : meta.getLastIncludedTerm();
      final long snapshotSizeBytes = directorySize(checkpointDir);
      snapshotBackend.onLoadCompleted(checkpointDir, snapshotIndex, snapshotTerm);
      lastSavedSnapshotIndex.set(snapshotIndex);
      lastSavedSnapshotTerm.set(snapshotTerm);
      lastSavedSnapshotSizeBytes.set(snapshotSizeBytes);
      snapshotProgress.onSnapshotLoaded(snapshotIndex, snapshotSizeBytes);
      snapshotBackend.afterLoad().join();
      loadExecutionAuthority(reader);
      snapshotConfigurationListener.accept(
          JRaftSnapshotElectionCertificate.read(Path.of(reader.getPath()), meta));
      LOG.info("Group {} loaded snapshot checkpoint from {}", groupId, checkpointDir);
      return true;
    } catch (final Exception e) {
      LOG.error("Group {} failed to load snapshot", groupId, e);
      return false;
    }
  }

  @Override
  public void onLeaderStart(final long term) {
    super.onLeaderStart(term);
    currentTerm.set(term);
    LOG.info("Group {} became leader at term {}", groupId, term);
    if (stateListener != null) {
      stateListener.onRoleChange(RaftRole.LEADER);
      // As leader, this node is its own leader; JRaftPartition resolves the concrete PeerId string.
      stateListener.onLeaderChange(null);
    }
  }

  @Override
  public void onLeaderStop(final Status status) {
    super.onLeaderStop(status);
    LOG.info("Group {} stepped down: {}", groupId, status);
    if (stateListener != null) {
      stateListener.onRoleChange(RaftRole.FOLLOWER);
    }
  }

  @Override
  public void onError(final RaftException e) {
    LOG.error("Group {} raft state machine error", groupId, e);
    // Escalate the fault so the partition can be marked FAULTED and reported to PD, rather than
    // going silently unavailable while PD still believes it is ACTIVE. This is just a listener
    // fan-out on the FSM thread; the business subscriber hops to its ConcurrencyControl (no I/O
    // here).
    final String reason = e.getStatus() != null ? e.getStatus().toString() : e.toString();
    stateListener.onFault(reason);
  }

  @Override
  public void onConfigurationCommitted(final Configuration configuration) {
    // Joint callbacks only signal the partition worker; never wait for a node lock here.
    configurationListener.run();
  }

  private void saveElectionCertificate(final SnapshotWriter writer) {
    JRaftSnapshotElectionCertificate.save(
        Path.of(writer.getPath()), writer.getCurrentMeta(), snapshotCertificateSupplier);
    if (!writer.addFile(JRaftSnapshotElectionCertificate.FILE)) {
      throw new IllegalStateException("Cannot register snapshot election certificate");
    }
  }

  private void saveExecutionAuthority(final SnapshotWriter writer) {
    if (executionAuthority == null) {
      return;
    }
    executionAuthority.saveSnapshot(Path.of(writer.getPath()));
    if (!writer.addFile(JRaftExecutionAuthority.FILE)) {
      throw new IllegalStateException("Cannot register execution authority snapshot");
    }
  }

  private void loadExecutionAuthority(final SnapshotReader reader) {
    if (executionAuthority != null) {
      executionAuthority.loadSnapshot(Path.of(reader.getPath()));
    }
  }

  // --- snapshots (D4/D5) -----------------------------------------------------------------------

  @Override
  public void onStopFollowing(final LeaderChangeContext ctx) {
    super.onStopFollowing(ctx);
    LOG.info("Group {} stopped following {}", groupId, ctx.getLeaderId());
    if (stateListener != null) {
      // Lost the current leader; leadership is unknown until the next election settles.
      stateListener.onLeaderChange(null);
    }
  }

  @Override
  public void onStartFollowing(final LeaderChangeContext ctx) {
    super.onStartFollowing(ctx);
    currentTerm.set(ctx.getTerm());
    LOG.info("Group {} started following {} at term {}", groupId, ctx.getLeaderId(), ctx.getTerm());
    if (stateListener != null) {
      stateListener.onRoleChange(RaftRole.FOLLOWER);
      final PeerId leader = ctx.getLeaderId();
      stateListener.onLeaderChange(leader == null ? null : leader.toString());
    }
  }

  public long lastSavedSnapshotIndex() {
    return lastSavedSnapshotIndex.get();
  }

  public long lastSavedSnapshotTerm() {
    return lastSavedSnapshotTerm.get();
  }

  public long lastSavedSnapshotSizeBytes() {
    return lastSavedSnapshotSizeBytes.get();
  }

  public long snapshotSaveGeneration() {
    return snapshotSaveGeneration.get();
  }

  /** All regular files under {@code snapshotRoot}, as names relative to it (forward slashes). */
  private static List<String> listSnapshotFiles(final Path snapshotRoot) throws IOException {
    try (final Stream<Path> paths = Files.walk(snapshotRoot)) {
      return paths
          .filter(Files::isRegularFile)
          .map(p -> snapshotRoot.relativize(p).toString().replace('\\', '/'))
          .toList();
    }
  }

  private static long directorySize(final Path directory) throws IOException {
    try (final Stream<Path> paths = Files.walk(directory)) {
      long bytes = 0;
      final var files = paths.filter(Files::isRegularFile).iterator();
      while (files.hasNext()) {
        bytes = Math.addExact(bytes, Files.size(files.next()));
      }
      return bytes;
    }
  }
}
