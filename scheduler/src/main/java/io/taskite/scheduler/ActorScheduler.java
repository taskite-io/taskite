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
package io.taskite.scheduler;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.DispatcherSelector;
import org.apache.pekko.actor.typed.PostStop;
import org.apache.pekko.actor.typed.Signal;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;

/**
 * Actor scheduler implementation. Provides asynchronous scheduling with CPU-bound and IO-bound
 * dispatcher separation. Compatible with the original ActorScheduler interface.
 */
public final class ActorScheduler implements AutoCloseable, ActorSchedulingService {

  // Upper bound on waiting for graceful actor closes during shutdown before terminating the actor
  // system regardless (see stop()).
  private static final long ACTOR_CLOSE_TIMEOUT_SECONDS = 5;

  private final AtomicReference<SchedulerState> state = new AtomicReference<>();
  private final int cpuThreadCount;
  private final int ioThreadCount;
  private final ConcurrentHashMap<String, ActorHandle> actorHandles = new ConcurrentHashMap<>();
  private ActorSystem<Void> system;
  private ScheduledExecutorService timerExecutor;
  private ActorTimerQueue timers;

  private ActorScheduler(final Builder builder) {
    cpuThreadCount = builder.cpuBoundThreadsCount;
    ioThreadCount = builder.ioBoundThreadsCount;
    state.set(SchedulerState.NEW);
  }

  /** Creates a new builder for this scheduler. */
  public static Builder newActorScheduler() {
    return new Builder();
  }

  @Override
  public ActorFuture<Void> submitActor(final Actor actor) {
    return submitActor(actor, SchedulingHints.cpuBound());
  }

  @Override
  public ActorFuture<Void> submitActor(final Actor actor, final SchedulingHints schedulingHints) {
    checkRunningState();

    final CompletableActorFuture<Void> future = new CompletableActorFuture<>();
    final DispatcherSelector dispatcher =
        schedulingHints == SchedulingHints.CPU_BOUND
            ? DispatcherSelector.defaultDispatcher()
            : DispatcherSelector.fromConfig("pekko.actor.io-dispatcher");
    final AtomicReference<ActorHandle> handleReference = new AtomicReference<>();

    final ActorRef<Object> actorRef =
        system.systemActorOf(
            Behaviors.setup(
                context ->
                    new ActorMailbox(
                        context,
                        actor,
                        future,
                        timers,
                        () -> {
                          final ActorHandle handle = handleReference.get();
                          if (handle != null) {
                            actorHandles.remove(actor.getName(), handle);
                          }
                        })),
            actor.getName(),
            dispatcher);
    final ActorHandle handle = new ActorHandle(actor, actorRef);
    handleReference.set(handle);
    actorHandles.put(actor.getName(), handle);
    if (actor.isActorClosed()) {
      actorHandles.remove(actor.getName(), handle);
    }
    return future;
  }

  int registeredActorCount() {
    return actorHandles.size();
  }

  /** Starts the scheduler. */
  public void start() {
    if (state.compareAndSet(SchedulerState.NEW, SchedulerState.RUNNING)) {
      system = createActorSystem();
      timers = new ActorTimerQueue();
      timerExecutor = Executors.newSingleThreadScheduledExecutor();
      timerExecutor.scheduleWithFixedDelay(timers::poll, 0, 1, TimeUnit.MILLISECONDS);
    } else {
      throw new IllegalStateException("Cannot start scheduler - already started");
    }
  }

  /** Stops the scheduler asynchronously. */
  public ActorFuture<Void> stop() {
    final CompletableActorFuture<Void> result = new CompletableActorFuture<>();
    if (state.compareAndSet(SchedulerState.RUNNING, SchedulerState.TERMINATING)) {
      final CompletableFuture<?>[] actorClosures =
          actorHandles.values().stream()
              .map(handle -> handle.actor().closeAsync().toCompletableFuture())
              .toArray(CompletableFuture[]::new);

      // Wait for graceful closes, but bound it: an actor that was submitted while the system was
      // already winding down may never process its CloseMessage (dead letter), so its close future
      // would never complete. Terminating the actor system regardless still stops those actors and
      // completes their close futures via the PostStop signal handler.
      CompletableFuture.allOf(actorClosures)
          .orTimeout(ACTOR_CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
          .handle(
              (ignored, error) -> {
                actorHandles.clear();
                if (timerExecutor != null) {
                  timerExecutor.shutdownNow();
                }
                timers.close();
                return null;
              })
          .thenCompose(
              ignored -> {
                system.terminate();
                return system.getWhenTerminated().thenApply(terminated -> null);
              })
          .thenRun(() -> state.set(SchedulerState.TERMINATED))
          .whenComplete(result);
      return result;
    } else {
      throw new IllegalStateException("Cannot stop scheduler - not running");
    }
  }

  @Override
  public void close() throws Exception {
    stop().get(10, TimeUnit.SECONDS);
  }

  private void checkRunningState() {
    if (state.get() != SchedulerState.RUNNING) {
      throw new IllegalStateException("Actor scheduler is not running");
    }
  }

  private ActorSystem<Void> createActorSystem() {
    final Config actorSystemConfig = buildActorSystemConfig();
    return ActorSystem.create(Behaviors.empty(), "actor-scheduler-system", actorSystemConfig);
  }

  private Config buildActorSystemConfig() {
    final Map<String, Object> configMap = new HashMap<>();

    configMap.put("pekko.actor.default-dispatcher.type", "Dispatcher");
    configMap.put("pekko.actor.default-dispatcher.executor", "fork-join-executor");
    configMap.put(
        "pekko.actor.default-dispatcher.fork-join-executor.parallelism-min", cpuThreadCount);
    configMap.put("pekko.actor.default-dispatcher.fork-join-executor.parallelism-factor", 1.0);
    configMap.put(
        "pekko.actor.default-dispatcher.fork-join-executor.parallelism-max", cpuThreadCount);
    configMap.put("pekko.actor.default-dispatcher.throughput", 100);

    configMap.put("pekko.actor.io-dispatcher.type", "Dispatcher");
    configMap.put("pekko.actor.io-dispatcher.executor", "thread-pool-executor");
    configMap.put(
        "pekko.actor.io-dispatcher.thread-pool-executor.core-pool-size-min", ioThreadCount);
    configMap.put("pekko.actor.io-dispatcher.thread-pool-executor.core-pool-size-factor", 1.0);
    configMap.put(
        "pekko.actor.io-dispatcher.thread-pool-executor.core-pool-size-max", ioThreadCount);
    configMap.put("pekko.actor.io-dispatcher.throughput", 1);

    // We own process shutdown explicitly (the application stops the scheduler and awaits actor
    // system termination). Disabling Pekko's own JVM shutdown hooks keeps the ordering
    // deterministic and avoids the "Could not addJvmShutdownHook, due to: Shutdown in progress"
    // warning that occurs when CoordinatedShutdown tries to register a hook from inside our hook.
    configMap.put("pekko.jvm-shutdown-hooks", "off");
    configMap.put("pekko.coordinated-shutdown.run-by-jvm-shutdown-hook", "off");

    return ConfigFactory.parseMap(configMap).withFallback(ConfigFactory.load());
  }

  /** Builder for creating an {@link ActorScheduler}. */
  public static final class Builder {
    private int cpuBoundThreadsCount = Math.max(1, Runtime.getRuntime().availableProcessors() - 2);
    private int ioBoundThreadsCount = 2;

    public Builder setCpuBoundActorThreadCount(final int count) {
      cpuBoundThreadsCount = count;
      return this;
    }

    public Builder setIoBoundActorThreadCount(final int count) {
      ioBoundThreadsCount = count;
      return this;
    }

    public ActorScheduler build() {
      return new ActorScheduler(this);
    }
  }

  private record ActorHandle(Actor actor, ActorRef<Object> actorRef) {}

  private static final class ActorMailbox extends AbstractBehavior<Object> {
    private final Actor actor;
    private final CompletableActorFuture<Void> startupFuture;
    private final ActorConcurrencyControl control;
    private final Runnable onStopped;

    ActorMailbox(
        final ActorContext<Object> context,
        final Actor actor,
        final CompletableActorFuture<Void> startupFuture,
        final ActorTimerQueue timers,
        final Runnable onStopped) {
      super(context);
      this.actor = actor;
      this.startupFuture = startupFuture;
      this.onStopped = onStopped;
      control = new ActorConcurrencyControl(context.getSelf(), timers);
      this.actor.bind(control, () -> context.getSelf().tell(CloseMessage.INSTANCE));
      initialize();
    }

    @Override
    public Receive<Object> createReceive() {
      return newReceiveBuilder()
          .onMessage(CloseMessage.class, ignored -> handleClose())
          .onAnyMessage(this::handleActorMessage)
          .onSignal(Signal.class, this::handleSignal)
          .build();
    }

    private void initialize() {
      try {
        actor.onActorStarting();
        actor.onActorStarted();
        startupFuture.complete(null);
      } catch (final Throwable error) {
        control.close();
        startupFuture.completeExceptionally(error);
        actor.onActorFailed();
        // The constructor is about to rethrow, so Behaviors.setup never returns a behavior and the
        // PostStop signal handler in createReceive() is never installed. Complete the close future
        // here so a caller awaiting closeAsync() (e.g. ActorScheduler.stop()) does not block on an
        // actor that failed before it could ever process a CloseMessage.
        actor.completeClose();
        actor.onActorClosed();
        onStopped.run();
        throw error;
      }
    }

    private Behavior<Object> handleActorMessage(final Object message) {
      try {
        if (message instanceof final Runnable runnable) {
          runnable.run();
        }
      } catch (final Throwable error) {
        actor.handleFailure(error);
      }
      return Behaviors.same();
    }

    private Behavior<Object> handleClose() {
      try {
        control.close();
        actor.onActorCloseRequested();
        actor.onActorClosing();
        actor.completeClose();
      } catch (final Throwable error) {
        actor.failClose(error);
        actor.handleFailure(error);
      }
      return Behaviors.stopped();
    }

    private Behavior<Object> handleSignal(final Signal signal) {
      if (signal instanceof PostStop) {
        try {
          control.close();
          // Complete the close future even if the actor stopped without processing a CloseMessage
          // (e.g. the message was dead-lettered because the actor system was already terminating).
          // Otherwise a caller awaiting closeAsync() — like ActorScheduler.stop() — would hang.
          actor.completeClose();
          actor.onActorClosed();
        } finally {
          onStopped.run();
        }
      }
      return Behaviors.same();
    }
  }

  private enum SchedulerState {
    NEW,
    RUNNING,
    TERMINATING,
    TERMINATED
  }

  private enum CloseMessage {
    INSTANCE
  }
}
