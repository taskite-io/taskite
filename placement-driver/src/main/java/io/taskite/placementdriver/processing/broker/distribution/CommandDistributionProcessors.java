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
package io.taskite.placementdriver.processing.broker.distribution;

import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.placementdriver.CommandDistributionIntent;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.processor.TypedRecordProcessors;

public final class CommandDistributionProcessors {
  private CommandDistributionProcessors() {}

  public static TypedRecordProcessors registerCommandDistributionProcessors(
      final TypedRecordProcessors processors, final TypedRecordProcessorContext context) {
    return processors
        .onCommand(
            ValueType.COMMAND_DISTRIBUTION,
            CommandDistributionIntent.START,
            new CommandDistributionStartProcessor(context))
        .onCommand(
            ValueType.COMMAND_DISTRIBUTION,
            CommandDistributionIntent.ACKNOWLEDGE,
            new CommandDistributionAcknowledgeProcessor(context))
        .onCommand(
            ValueType.COMMAND_DISTRIBUTION,
            CommandDistributionIntent.COMPLETE,
            new CommandDistributionCompleteProcessor(context))
        .onCommand(
            ValueType.COMMAND_DISTRIBUTION,
            CommandDistributionIntent.QUERY,
            new CommandDistributionQueryProcessor(context))
        .onCommand(
            ValueType.COMMAND_DISTRIBUTION,
            CommandDistributionIntent.LIST,
            new CommandDistributionListProcessor(context))
        .onCommand(
            ValueType.COMMAND_DISTRIBUTION,
            CommandDistributionIntent.REDISTRIBUTE,
            new CommandDistributionRedistributeProcessor(context));
  }
}
