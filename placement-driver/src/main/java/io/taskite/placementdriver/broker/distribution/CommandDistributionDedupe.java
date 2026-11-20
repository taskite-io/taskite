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
package io.taskite.placementdriver.broker.distribution;

import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRequestRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionDeletionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionRecord;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.ValueType;

public final class CommandDistributionDedupe {
  private CommandDistributionDedupe() {}

  public static String dedupeKey(final CommandDistributionRequestRecord<?> request) {
    return key(request.getValueType(), request.getValue());
  }

  public static String dedupeKey(final CommandDistributionRecord<?> record) {
    return key(record.getValueType(), record.getValue());
  }

  private static String key(final ValueType type, final UnifiedRecordValue value) {
    return switch (type) {
      case JOB_SUBSCRIPTION -> {
        final var subscription = (JobSubscriptionRecord) value;
        yield subscriptionDedupeKey(
            subscription.getClusterId(), subscription.getTenantId(), subscription.getJobType());
      }
      case JOB_SUBSCRIPTION_DELETION -> deletionDedupeKey((JobSubscriptionDeletionRecord) value);
      default -> throw new IllegalArgumentException("Unsupported distribution type: " + type);
    };
  }

  /** Identifies the unique job subscription within a cluster and tenant. */
  public static String subscriptionDedupeKey(
      final String clusterId, final String tenantId, final String jobType) {
    return "create:" + field(clusterId) + field(tenantId) + field(jobType);
  }

  public static boolean sameCommandValue(
      final CommandDistributionRequestRecord<?> request,
      final CommandDistributionRecord<?> stored) {
    return request.getValueType() == stored.getValueType()
        && request.getIntent() == stored.getIntent()
        && request.getValue().toJson().equals(stored.getValue().toJson());
  }

  public static boolean targetsSubscription(
      final JobSubscriptionDeletionRecord deletion, final CommandDistributionRecord<?> target) {
    return target.getValueType() == ValueType.JOB_SUBSCRIPTION
        && target.getDistributionKey() == deletion.getSubscriptionKey()
        && target.getValue() instanceof JobSubscriptionRecord subscription
        && deletion.getClusterId().equals(subscription.getClusterId())
        && deletion.getTenantId().equals(subscription.getTenantId());
  }

  public static String deletionDedupeKey(final JobSubscriptionDeletionRecord record) {
    return "delete:"
        + field(record.getClusterId())
        + field(record.getTenantId())
        + record.getSubscriptionKey();
  }

  private static String field(final String value) {
    return value.length() + ":" + value;
  }
}
