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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.EnumOutter.ErrorType;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import io.taskite.cluster.raft.RaftRole;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * L5: a raft state-machine error must escalate as a partition fault (so PD can mark it FAULTED)
 * rather than being silently logged.
 */
final class JRaftStateMachineFaultTest {

  @Test
  void onErrorFansOutFaultWithStatusReason() {
    final AtomicReference<String> fault = new AtomicReference<>();
    final JRaftStateListener listener =
        new JRaftStateListener() {
          @Override
          public void onRoleChange(final RaftRole role) {}

          @Override
          public void onLeaderChange(final String leaderId) {}

          @Override
          public void onFault(final String reason) {
            fault.set(reason);
          }
        };
    final JRaftStateMachine fsm = new JRaftStateMachine("group-1", listener);

    fsm.onError(
        new RaftException(
            ErrorType.ERROR_TYPE_STATE_MACHINE,
            new Status(RaftError.ESTATEMACHINE, "apply failed: boom")));

    assertNotNull(fault.get(), "fault must have fired");
    assertTrue(fault.get().contains("apply failed: boom"), "fault reason: " + fault.get());
  }

  @Test
  void onFaultDefaultsToNoOpForListenersThatIgnoreIt() {
    // A listener that only implements role/leader must not break when a fault is escalated.
    final AtomicReference<RaftRole> role = new AtomicReference<>();
    final JRaftStateListener listener =
        new JRaftStateListener() {
          @Override
          public void onRoleChange(final RaftRole newRole) {
            role.set(newRole);
          }

          @Override
          public void onLeaderChange(final String leaderId) {}
        };
    final JRaftStateMachine fsm = new JRaftStateMachine("group-2", listener);

    fsm.onError(new RaftException(ErrorType.ERROR_TYPE_STATE_MACHINE));

    assertEquals(null, role.get(), "role listener untouched by a fault");
  }
}
