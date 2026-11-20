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

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRequestRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.placementdriver.JobSubscriptionIntent;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.writer.Writers;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

final class JobSubscriptionValidationTest {
  @ParameterizedTest
  @ValueSource(strings = {"[]", "null", "{broken", "{} {}", "{} true"})
  void rejectsMalformedOrMultipleVariableDocumentsBeforeDistribution(final String variables) {
    final var state = mock(ProcessingState.class, RETURNS_DEEP_STUBS);
    final var writers = mock(Writers.class, RETURNS_DEEP_STUBS);
    final var context = mock(TypedRecordProcessorContext.class);
    when(context.getWriters()).thenReturn(writers);
    when(context.getProcessingState(ProcessingState.class)).thenReturn(state);
    when(context.getKeyGenerator()).thenReturn(() -> 42);
    final var request =
        new CommandDistributionRequestRecord<JobSubscriptionRecord>()
            .setValueType(ValueType.JOB_SUBSCRIPTION)
            .setIntent(JobSubscriptionIntent.CREATE)
            .setValue(
                new JobSubscriptionRecord()
                    .setClusterId("demo")
                    .setTenantId("tenant")
                    .setName("charge")
                    .setJobType("charge")
                    .setVariables(variables));
    final TypedRecord<CommandDistributionRequestRecord<?>> command = mock(TypedRecord.class);
    doReturn(request).when(command).getValue();

    new CommandDistributionStartProcessor(context).processRecord(command);

    verify(writers.rejection()).appendRejection(eq(RejectionType.INVALID_ARGUMENT), anyString());
  }
}
