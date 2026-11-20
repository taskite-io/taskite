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
package io.taskite.engine.processing.distribution;

import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionDeletionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.placementdriver.CommandDistributionIntent;
import io.taskite.protocol.record.intent.placementdriver.JobSubscriptionIntent;
import io.taskite.protocol.record.value.broker.TenantOwned;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.writer.Writers;

public final class CommandDistributionAcknowledgeProcessor
    implements TypedRecordProcessor<CommandDistributionRecord<?>> {
  private static final String INVALID_DISTRIBUTION_KEY =
      "Expected to acknowledge command distribution for partition '%d' for tenant '%s' with a positive distribution"
          + " key, but received '%d'";
  private static final String INVALID_DISTRIBUTION_KEY_WITHOUT_TENANT =
      "Expected to acknowledge command distribution for partition '%d' with a positive distribution"
          + " key, but received '%d'";
  private static final String UNSUPPORTED_VALUE_TYPE =
      "Expected to acknowledge command distribution with key '%d' for tenant '%s' for job subscription creation or"
          + " deletion, but received value type '%s'";
  private static final String UNSUPPORTED_VALUE_TYPE_WITHOUT_TENANT =
      "Expected to acknowledge command distribution with key '%d' for job subscription creation or"
          + " deletion, but received value type '%s'";

  private final Writers writers;

  public CommandDistributionAcknowledgeProcessor(final Writers writers) {
    this.writers = writers;
  }

  @Override
  public void processRecord(final TypedRecord<CommandDistributionRecord<?>> record) {
    final CommandDistributionRecord<?> value = record.getValue();
    final String tenantId =
        value.getValue() instanceof TenantOwned tenant ? tenant.getTenantId() : null;
    if (value.getDistributionKey() <= 0) {
      reject(
          record,
          RejectionType.INVALID_ARGUMENT,
          tenantId == null
              ? INVALID_DISTRIBUTION_KEY_WITHOUT_TENANT.formatted(
                  record.getPartitionId(), value.getDistributionKey())
              : INVALID_DISTRIBUTION_KEY.formatted(
                  record.getPartitionId(), tenantId, value.getDistributionKey()));
      return;
    }
    if (value.getValueType() != ValueType.JOB_SUBSCRIPTION
        && value.getValueType() != ValueType.JOB_SUBSCRIPTION_DELETION) {
      reject(
          record,
          RejectionType.INVALID_ARGUMENT,
          tenantId == null
              ? UNSUPPORTED_VALUE_TYPE_WITHOUT_TENANT.formatted(
                  value.getDistributionKey(), value.getValueType())
              : UNSUPPORTED_VALUE_TYPE.formatted(
                  value.getDistributionKey(), tenantId, value.getValueType()));
      return;
    }
    if (value.getPartitionId() <= 0) {
      value.setPartitionId(record.getPartitionId());
    }
    if (value.getValueType() == ValueType.JOB_SUBSCRIPTION) {
      writers
          .state()
          .appendFollowUpEvent(
              value.getDistributionKey(),
              ValueType.JOB_SUBSCRIPTION,
              JobSubscriptionIntent.CREATED,
              (JobSubscriptionRecord) value.getValue());
    } else {
      final var deletion = (JobSubscriptionDeletionRecord) value.getValue();
      if (deletion.getSubscriptionKey() <= 0) {
        reject(record, RejectionType.INVALID_ARGUMENT, "Subscription key must be positive");
        return;
      }
      writers
          .state()
          .appendFollowUpEvent(
              deletion.getSubscriptionKey(),
              ValueType.JOB_SUBSCRIPTION_DELETION,
              JobSubscriptionIntent.DELETED,
              deletion);
    }
    writers
        .state()
        .appendFollowUpEvent(
            value.getDistributionKey(), CommandDistributionIntent.ACKNOWLEDGED, value);
    writers
        .response()
        .writeEventOnCommand(
            value.getDistributionKey(), CommandDistributionIntent.ACKNOWLEDGED, value, record);
  }

  private void reject(
      final TypedRecord<CommandDistributionRecord<?>> command,
      final RejectionType type,
      final String reason) {
    writers.rejection().appendRejection(type, reason);
    writers.response().writeRejectionOnCommand(command, type, reason);
  }
}
