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
package io.taskite.engine.processing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import io.taskite.engine.processing.distribution.CommandDistributionAcknowledgeProcessor;
import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionDeletionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.protocol.record.intent.placementdriver.CommandDistributionIntent;
import io.taskite.protocol.record.intent.placementdriver.JobSubscriptionIntent;
import io.taskite.streamplatform.api.writer.PostCommitActionWriter;
import io.taskite.streamplatform.api.writer.StateWriter;
import io.taskite.streamplatform.api.writer.TypedCommandWriter;
import io.taskite.streamplatform.api.writer.TypedRejectionWriter;
import io.taskite.streamplatform.api.writer.TypedResponseWriter;
import io.taskite.streamplatform.api.writer.Writers;
import org.junit.jupiter.api.Test;

final class CommandDistributionAcknowledgeProcessorTest {
  private static final long DISTRIBUTION_KEY = 101;

  @Test
  void acknowledgesSubscriptionAndUsesCommandPartition() {
    final CapturingWriters writers = new CapturingWriters();
    final CommandDistributionRecord<JobSubscriptionRecord> distribution = deployment();

    new CommandDistributionAcknowledgeProcessor(writers).processRecord(record(distribution, 3));

    assertEquals(3, distribution.getPartitionId());
    assertEquals(DISTRIBUTION_KEY, writers.stateKey);
    assertEquals(CommandDistributionIntent.ACKNOWLEDGED, writers.stateIntent);
    assertSame(distribution, writers.stateValue);
    assertEquals(DISTRIBUTION_KEY, writers.responseKey);
    assertEquals(CommandDistributionIntent.ACKNOWLEDGED, writers.responseIntent);
    assertSame(distribution, writers.responseValue);
    assertNull(writers.rejectionType);
    assertNull(writers.responseRejectionType);
  }

  @Test
  void preservesExplicitPartitionForDeletion() {
    final CapturingWriters writers = new CapturingWriters();
    final CommandDistributionRecord<JobSubscriptionDeletionRecord> distribution =
        deletion().setPartitionId(2);

    new CommandDistributionAcknowledgeProcessor(writers).processRecord(record(distribution, 3));

    assertEquals(2, distribution.getPartitionId());
    assertEquals(CommandDistributionIntent.ACKNOWLEDGED, writers.stateIntent);
    assertEquals(CommandDistributionIntent.ACKNOWLEDGED, writers.responseIntent);
  }

  @Test
  void rejectsNonPositiveDistributionKeyWithoutWritingEvent() {
    final CapturingWriters writers = new CapturingWriters();
    final CommandDistributionRecord<JobSubscriptionRecord> distribution =
        deployment().setDistributionKey(0);

    new CommandDistributionAcknowledgeProcessor(writers).processRecord(record(distribution, 1));

    assertEquals(RejectionType.INVALID_ARGUMENT, writers.rejectionType);
    assertEquals(RejectionType.INVALID_ARGUMENT, writers.responseRejectionType);
    assertEquals(
        "Expected to acknowledge command distribution for partition '1' for tenant 'tenant'"
            + " with a positive distribution key, but received '0'",
        writers.rejectionReason);
    assertEquals(writers.rejectionReason, writers.responseRejectionReason);
    assertNull(writers.stateIntent);
    assertNull(writers.responseIntent);
  }

  @Test
  void rejectsUnsupportedValueTypeWithoutWritingEvent() {
    final CapturingWriters writers = new CapturingWriters();
    final CommandDistributionRecord<JobRecord> distribution =
        new CommandDistributionRecord<JobRecord>()
            .setDistributionKey(DISTRIBUTION_KEY)
            .setClusterId("demo")
            .setValueType(ValueType.JOB)
            .setValue(new JobRecord().setTenantId("tenant-job"));

    new CommandDistributionAcknowledgeProcessor(writers).processRecord(record(distribution, 1));

    assertEquals(RejectionType.INVALID_ARGUMENT, writers.rejectionType);
    assertEquals(RejectionType.INVALID_ARGUMENT, writers.responseRejectionType);
    assertEquals(
        "Expected to acknowledge command distribution with key '101' for tenant 'tenant-job'"
            + " for job subscription creation or deletion, but received value type 'JOB'",
        writers.rejectionReason);
    assertEquals(writers.rejectionReason, writers.responseRejectionReason);
    distribution.setValue(null);
    new CommandDistributionAcknowledgeProcessor(writers).processRecord(record(distribution, 1));
    assertEquals(RejectionType.INVALID_ARGUMENT, writers.rejectionType);
    assertEquals(RejectionType.INVALID_ARGUMENT, writers.responseRejectionType);
    assertEquals(
        "Expected to acknowledge command distribution with key '101' for job subscription creation or"
            + " deletion, but received value type 'JOB'",
        writers.rejectionReason);
    assertEquals(writers.rejectionReason, writers.responseRejectionReason);
    assertNull(writers.stateIntent);
    assertNull(writers.responseIntent);
  }

  private static CommandDistributionRecord<JobSubscriptionRecord> deployment() {
    return new CommandDistributionRecord<JobSubscriptionRecord>()
        .setDistributionKey(101)
        .setClusterId("demo")
        .setValueType(ValueType.JOB_SUBSCRIPTION)
        .setValue(
            new JobSubscriptionRecord()
                .setClusterId("demo")
                .setTenantId("tenant")
                .setName("subscription")
                .setJobType("work"));
  }

  private static CommandDistributionRecord<JobSubscriptionDeletionRecord> deletion() {
    return new CommandDistributionRecord<JobSubscriptionDeletionRecord>()
        .setDistributionKey(102)
        .setClusterId("demo")
        .setValueType(ValueType.JOB_SUBSCRIPTION_DELETION)
        .setIntent(JobSubscriptionIntent.DELETE)
        .setValue(
            new JobSubscriptionDeletionRecord()
                .setClusterId("demo")
                .setTenantId("tenant")
                .setSubscriptionKey(DISTRIBUTION_KEY));
  }

  private static TypedRecord<CommandDistributionRecord<?>> record(
      final CommandDistributionRecord<?> value, final int partitionId) {
    return new TypedRecord<>() {
      @Override
      public long getKey() {
        return value.getDistributionKey();
      }

      @Override
      public CommandDistributionRecord<?> getValue() {
        return value;
      }

      @Override
      public String getRequestStreamId() {
        return "stream";
      }

      @Override
      public String getRequestId() {
        return "request";
      }

      @Override
      public int getLength() {
        return value.getLength();
      }

      @Override
      public int getPartitionId() {
        return partitionId;
      }
    };
  }

  private static final class CapturingWriters implements Writers {
    private long stateKey;
    private Intent stateIntent;
    private UnifiedRecordValue stateValue;
    private long responseKey;
    private Intent responseIntent;
    private UnifiedRecordValue responseValue;
    private RejectionType rejectionType;
    private RejectionType responseRejectionType;
    private String rejectionReason;
    private String responseRejectionReason;

    @Override
    public TypedCommandWriter command() {
      return (key, intent, value) -> {};
    }

    @Override
    public StateWriter state() {
      return new StateWriter() {
        @Override
        public void appendFollowUpEvent(
            final long key, final Intent intent, final UnifiedRecordValue value) {
          stateKey = key;
          stateIntent = intent;
          stateValue = value;
        }

        @Override
        public void appendFollowUpEvent(
            final long key,
            final Intent intent,
            final UnifiedRecordValue value,
            final int recordVersion) {
          appendFollowUpEvent(key, intent, value);
        }

        @Override
        public void appendFollowUpEvent(
            final long key,
            final ValueType valueType,
            final Intent intent,
            final UnifiedRecordValue value) {
          appendFollowUpEvent(key, intent, value);
        }

        @Override
        public void appendFollowUpEvent(
            final long key,
            final ValueType valueType,
            final Intent intent,
            final UnifiedRecordValue value,
            final int recordVersion) {
          appendFollowUpEvent(key, intent, value);
        }
      };
    }

    @Override
    public TypedRejectionWriter rejection() {
      return (type, reason) -> {
        rejectionType = type;
        rejectionReason = reason;
      };
    }

    @Override
    public TypedResponseWriter response() {
      return new TypedResponseWriter() {
        @Override
        public void writeEventOnCommand(
            final long eventKey,
            final Intent eventIntent,
            final UnifiedRecordValue eventValue,
            final TypedRecord<?> sourceCommand) {
          responseKey = eventKey;
          responseIntent = eventIntent;
          responseValue = eventValue;
        }

        @Override
        public void writeRejectionOnCommand(
            final TypedRecord<?> sourceCommand, final RejectionType type, final String reason) {
          responseRejectionType = type;
          responseRejectionReason = reason;
        }
      };
    }

    @Override
    public PostCommitActionWriter postCommit() {
      return action -> {};
    }
  }
}
