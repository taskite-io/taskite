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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.alipay.sofa.jraft.ReplicatorGroup;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.ReplicatorGroupImpl;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RemotingException;
import com.alipay.sofa.jraft.option.RpcOptions;
import com.alipay.sofa.jraft.rpc.InvokeCallback;
import com.alipay.sofa.jraft.rpc.InvokeContext;
import com.alipay.sofa.jraft.rpc.RpcClient;
import com.alipay.sofa.jraft.rpc.RpcRequests.ErrorResponse;
import com.alipay.sofa.jraft.rpc.RpcRequests.GetFileRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.PingRequest;
import com.alipay.sofa.jraft.rpc.RpcResponseClosureAdapter;
import com.alipay.sofa.jraft.rpc.impl.AbstractClientService;
import com.alipay.sofa.jraft.rpc.impl.GrpcClient;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.RpcFactoryHelper;
import com.google.protobuf.Message;
import io.taskite.cluster.raft.RaftPeer;
import io.taskite.cluster.raft.jraft.membership.JRaftPeerMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

final class JRaftResolvingRpcClientTest {
  private static final JRaftPeerMapper MAPPER = JRaftPeerMapper.logical("demo");
  private static final String GROUP = "demo-partition-0";
  private static final Endpoint IDENTITY =
      MAPPER.peerId(new RaftPeer("broker-0", "127.0.0.1", 26502)).getEndpoint();
  private static final Endpoint ORIGINAL = new Endpoint("127.0.0.1", 26502);
  private static final Endpoint CURRENT = new Endpoint("[::1]", 36502);

  @Test
  void unresolvedMemberUsesConnectionAndRemotingFailureWithoutCallingTransportOrCallback()
      throws Exception {
    final var directory = new JRaftEndpointDirectory(GROUP, List.of(), MAPPER);
    final var transport = new RecordingClient();
    final var client = new JRaftResolvingRpcClient(transport, directory);
    final var callbacks = new AtomicInteger();
    assertFalse(client.checkConnection(IDENTITY));
    assertFalse(client.checkConnection(IDENTITY, true));
    final var failure =
        assertThrows(
            RemotingException.class, () -> client.invokeSync(IDENTITY, new Object(), null, 100));
    assertEquals(
        "Unknown Raft member in group directory: group=" + GROUP + ", peer=" + IDENTITY,
        failure.getMessage());
    assertInstanceOf(IllegalStateException.class, failure.getCause());
    assertThrows(
        RemotingException.class,
        () ->
            client.invokeAsync(
                IDENTITY, new Object(), null, (value, error) -> callbacks.incrementAndGet(), 100));
    client.closeConnection(IDENTITY);
    assertEquals(0, callbacks.get());
    assertEquals(null, transport.destination);
    client.shutdown();
  }

  @Test
  void callbackExceptionPropagatesWithoutRepeatingCallback() {
    final var directory = new JRaftEndpointDirectory(GROUP, List.of(peer(ORIGINAL)), MAPPER);
    final var client = new JRaftResolvingRpcClient(new RecordingClient(), directory);
    final var callbacks = new AtomicInteger();
    assertThrows(
        IllegalArgumentException.class,
        () ->
            client.invokeAsync(
                IDENTITY,
                new Object(),
                null,
                (value, error) -> {
                  callbacks.incrementAndGet();
                  throw new IllegalArgumentException("callback failed");
                },
                100));
    assertEquals(1, callbacks.get());
    client.shutdown();
  }

  @Test
  void resolvedTransportRuntimeFailureIsNotReclassifiedAsMissingMember() {
    final var directory = new JRaftEndpointDirectory(GROUP, List.of(peer(ORIGINAL)), MAPPER);
    final var failure = new IllegalStateException("transport failed");
    final var transport =
        new RecordingClient() {
          @Override
          public boolean checkConnection(final Endpoint endpoint, final boolean create) {
            throw failure;
          }

          @Override
          public Object invokeSync(
              final Endpoint endpoint,
              final Object request,
              final InvokeContext context,
              final long timeout) {
            throw failure;
          }
        };
    final var client = new JRaftResolvingRpcClient(transport, directory);
    try {
      assertSame(
          failure,
          assertThrows(IllegalStateException.class, () -> client.checkConnection(IDENTITY, true)));
      assertSame(
          failure,
          assertThrows(
              IllegalStateException.class,
              () -> client.invokeSync(IDENTITY, new Object(), null, 100)));
    } finally {
      client.shutdown();
    }
  }

  @Test
  void resolvesRequestsAndInvalidatesPreviousAddressWithoutChangingRequest() throws Exception {
    final var directory = new JRaftEndpointDirectory(GROUP, List.of(peer(ORIGINAL)), MAPPER);
    final var transport = new RecordingClient();
    final var client = new JRaftResolvingRpcClient(transport, directory);
    final Object request = new Object();
    client.invokeSync(IDENTITY, request, null, 100);
    assertEquals(ORIGINAL, transport.destination);
    assertSame(request, transport.request);
    directory.update(List.of(peer(CURRENT)));
    assertEquals(List.of(ORIGINAL), transport.closed);
    client.invokeAsync(IDENTITY, request, null, (response, error) -> {}, 100);
    assertEquals(CURRENT, transport.destination);
    assertSame(request, transport.request);
    client.checkConnection(IDENTITY, true);
    assertEquals(CURRENT, transport.destination);
    directory.remember(List.of(peer(ORIGINAL)));
    assertEquals(CURRENT, directory.resolve(IDENTITY));
    client.shutdown();
    directory.update(List.of(peer(ORIGINAL)));
    assertEquals(List.of(ORIGINAL), transport.closed);
  }

  @Test
  void snapshotFileRequestUsesPhysicalUriWithoutMemberResolution() throws Exception {
    final var directory = new JRaftEndpointDirectory(GROUP, List.of(peer(CURRENT)), MAPPER);
    final var transport = new RecordingClient();
    final var client = new JRaftResolvingRpcClient(transport, directory);
    client.invokeSync(CURRENT, GetFileRequest.getDefaultInstance(), null, 100);
    assertEquals(CURRENT, transport.destination);
    client.invokeAsync(
        ORIGINAL, GetFileRequest.getDefaultInstance(), null, (value, error) -> {}, 100);
    assertEquals(ORIGINAL, transport.destination);
    client.shutdown();
  }

  @Test
  void physicalConnectCallbackResolvesOnlyCurrentLogicalMember() {
    final var directory = new JRaftEndpointDirectory(GROUP, List.of(peer(ORIGINAL)), MAPPER);
    final var transport = new RecordingClient();
    final var client = new JRaftResolvingRpcClient(transport, directory);
    final AtomicReference<PeerId> connected = new AtomicReference<>();
    client.registerConnectEventListener(
        new ReplicatorGroupImpl() {
          @Override
          public void checkReplicator(final PeerId peer, final boolean lock) {
            connected.set(peer);
          }
        });
    directory.update(List.of(peer(CURRENT)));
    transport.listener.checkReplicator(new PeerId(ORIGINAL, 0), true);
    assertEquals(null, connected.get());
    transport.listener.checkReplicator(new PeerId(CURRENT, 0), true);
    assertEquals(new PeerId(IDENTITY, 0), connected.get());
    client.shutdown();
  }

  @Test
  void directoriesAreIsolatedAndUnknownIdentityCannotBecomeADialTarget() {
    final var first = new JRaftEndpointDirectory(GROUP, List.of(peer(ORIGINAL)), MAPPER);
    final var second = new JRaftEndpointDirectory(GROUP, List.of(peer(CURRENT)), MAPPER);
    first.update(List.of(peer(new Endpoint("127.0.0.2", 46502))));
    assertEquals(CURRENT, second.resolve(IDENTITY));
    assertThrows(IllegalStateException.class, () -> second.resolve(ORIGINAL));
  }

  @Test
  void factorySpiScopesOnlyBrokerGroupClientsAndRestoresScopeOnFailure() {
    assertInstanceOf(JRaftLogicalRpcFactory.class, RpcFactoryHelper.rpcFactory());
    final var factory = new JRaftLogicalRpcFactory();
    final var directory = new JRaftEndpointDirectory(GROUP, List.of(peer(ORIGINAL)), MAPPER);
    final var client = JRaftLogicalRpcFactory.inGroup(directory, factory::createRpcClient);
    assertInstanceOf(JRaftResolvingRpcClient.class, client);
    client.shutdown();
    assertThrows(
        IllegalStateException.class,
        () ->
            JRaftLogicalRpcFactory.inGroup(
                directory,
                () -> {
                  throw new IllegalStateException("construction failed");
                }));
    final var raw = factory.createRpcClient();
    assertInstanceOf(GrpcClient.class, raw);
    raw.shutdown();
  }

  @Test
  void nestedFactoryScopesRestoreLogicalTransportAfterPhysicalSnapshotClient() {
    final var factory = new JRaftLogicalRpcFactory();
    final var directory = new JRaftEndpointDirectory(GROUP, List.of(peer(ORIGINAL)), MAPPER);
    JRaftLogicalRpcFactory.inGroup(
        directory,
        () -> {
          final var fileClient = JRaftLogicalRpcFactory.withoutGroup(factory::createRpcClient);
          assertInstanceOf(GrpcClient.class, fileClient);
          fileClient.shutdown();
          assertThrows(
              IllegalStateException.class,
              () ->
                  JRaftLogicalRpcFactory.withoutGroup(
                      () -> {
                        throw new IllegalStateException("snapshot client failed");
                      }));
          final var memberClient = factory.createRpcClient();
          assertInstanceOf(JRaftResolvingRpcClient.class, memberClient);
          memberClient.shutdown();
          return null;
        });
    final var raw = factory.createRpcClient();
    assertInstanceOf(GrpcClient.class, raw);
    raw.shutdown();
  }

  @Test
  void jraftServiceCompletesMissingEndpointFailureOnceAndReconnectsAfterTopologyUpdate()
      throws Exception {
    final var directory = new JRaftEndpointDirectory(GROUP, List.of(), MAPPER);
    final var transport = new RecordingClient();
    final var client = new JRaftResolvingRpcClient(transport, directory);
    final var service = new TestClientService(client);
    final var callbacks = new AtomicInteger();
    final var statuses = new ArrayList<Status>();
    final var done =
        new RpcResponseClosureAdapter<Message>() {
          @Override
          public void run(final Status status) {
            callbacks.incrementAndGet();
            statuses.add(status);
          }
        };
    final var request = PingRequest.newBuilder().setSendTimestamp(1).build();
    try {
      assertFalse(service.connect(IDENTITY));
      final var failed = service.invokeWithDone(IDENTITY, request, done, 100, Runnable::run);
      final var error =
          assertThrows(ExecutionException.class, () -> failed.get(1, TimeUnit.SECONDS));
      assertInstanceOf(RemotingException.class, error.getCause());
      assertEquals(1, callbacks.get());
      assertFalse(statuses.getFirst().isOk());
      assertEquals(null, transport.destination);
      directory.update(List.of(peer(CURRENT)));
      assertEquals(true, service.connect(IDENTITY));
      service.invokeWithDone(IDENTITY, request, done, 100, Runnable::run).get(1, TimeUnit.SECONDS);
      assertEquals(2, callbacks.get());
      assertEquals(true, statuses.getLast().isOk());
      assertEquals(CURRENT, transport.destination);
    } finally {
      client.shutdown();
    }
  }

  private static final class TestClientService extends AbstractClientService {
    TestClientService(final RpcClient client) {
      rpcClient = client;
      rpcOptions = new RpcOptions();
    }
  }

  private static RaftPeer peer(final Endpoint endpoint) {
    return new RaftPeer("broker-0", endpoint.getIp(), endpoint.getPort(), true, 100);
  }

  private static class RecordingClient implements RpcClient {
    private Endpoint destination;
    private Object request;
    private ReplicatorGroup listener;
    private final List<Endpoint> closed = new ArrayList<>();

    @Override
    public boolean init(final RpcOptions options) {
      return true;
    }

    @Override
    public void shutdown() {}

    @Override
    public boolean checkConnection(final Endpoint endpoint) {
      return checkConnection(endpoint, false);
    }

    @Override
    public boolean checkConnection(final Endpoint endpoint, final boolean create) {
      destination = endpoint;
      return true;
    }

    @Override
    public void closeConnection(final Endpoint endpoint) {
      closed.add(endpoint);
    }

    @Override
    public void registerConnectEventListener(final ReplicatorGroup group) {
      listener = group;
    }

    @Override
    public Object invokeSync(
        final Endpoint endpoint,
        final Object value,
        final InvokeContext context,
        final long timeout) {
      destination = endpoint;
      request = value;
      return value instanceof PingRequest
          ? ErrorResponse.newBuilder().setErrorCode(0).build()
          : value;
    }

    @Override
    public void invokeAsync(
        final Endpoint endpoint,
        final Object value,
        final InvokeContext context,
        final InvokeCallback callback,
        final long timeout) {
      destination = endpoint;
      request = value;
      callback.complete(value, null);
    }
  }
}
