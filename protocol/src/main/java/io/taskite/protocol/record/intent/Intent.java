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
package io.taskite.protocol.record.intent;

import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.BrokerPartitionConfigurationIntent;
import io.taskite.protocol.record.intent.broker.BrokerPartitionOperationIntent;
import io.taskite.protocol.record.intent.broker.IncidentIntent;
import io.taskite.protocol.record.intent.broker.JobBatchIntent;
import io.taskite.protocol.record.intent.broker.JobIntent;
import io.taskite.protocol.record.intent.broker.JobYieldIntent;
import io.taskite.protocol.record.intent.broker.VariableDocumentIntent;
import io.taskite.protocol.record.intent.broker.VariableIntent;
import io.taskite.protocol.record.intent.common.TimerIntent;
import io.taskite.protocol.record.intent.placementdriver.BrokerIntent;
import io.taskite.protocol.record.intent.placementdriver.BrokerPartitionChangeIntent;
import io.taskite.protocol.record.intent.placementdriver.CommandDistributionIntent;
import io.taskite.protocol.record.intent.placementdriver.JobSubscriptionIntent;
import io.taskite.protocol.record.intent.placementdriver.JobSubscriptionMirrorIntent;
import io.taskite.protocol.record.intent.placementdriver.MemberChangeIntent;
import io.taskite.protocol.record.intent.placementdriver.TopologyIntent;
import io.taskite.protocol.record.intent.placementdriver.WorkerIntent;

public interface Intent {
  short NULL_VAL = (short) 255;
  Intent UNKNOWN = UnknownIntent.UNKNOWN;

  short value();

  String name();

  boolean isEvent();

  static Intent fromProtocolValue(final ValueType valueType, final short intent) {
    if (intent == NULL_VAL) {
      return Intent.UNKNOWN;
    }
    return switch (valueType) {
      case TOPOLOGY -> TopologyIntent.from(intent);
      case BROKER_REGISTRATION -> BrokerIntent.from(intent);
      case MEMBER_CHANGE -> MemberChangeIntent.from(intent);
      case PARTITION_CHANGE -> BrokerPartitionChangeIntent.from(intent);
      case BROKER_PARTITION_CONFIGURATION -> BrokerPartitionConfigurationIntent.from(intent);
      case BROKER_PARTITION_OPERATION -> BrokerPartitionOperationIntent.from(intent);
      case COMMAND_DISTRIBUTION -> CommandDistributionIntent.from(intent);
      case JOB_SUBSCRIPTION, JOB_SUBSCRIPTION_DELETION -> JobSubscriptionIntent.from(intent);
      case JOB_SUBSCRIPTION_MIRROR -> JobSubscriptionMirrorIntent.from(intent);
      case WORKER_REGISTRATION -> WorkerIntent.from(intent);
      case JOB -> JobIntent.from(intent);
      case JOB_BATCH -> JobBatchIntent.from(intent);
      case JOB_YIELD -> JobYieldIntent.from(intent);
      case INCIDENT -> IncidentIntent.from(intent);
      case VARIABLE -> VariableIntent.from(intent);
      case VARIABLE_DOCUMENT -> VariableDocumentIntent.from(intent);
      case TIMER -> TimerIntent.from(intent);
      case NULL_VAL, SBE_UNKNOWN -> Intent.UNKNOWN;
    };
  }

  enum UnknownIntent implements Intent {
    UNKNOWN;

    @Override
    public short value() {
      return NULL_VAL;
    }

    @Override
    public boolean isEvent() {
      return false;
    }
  }
}
