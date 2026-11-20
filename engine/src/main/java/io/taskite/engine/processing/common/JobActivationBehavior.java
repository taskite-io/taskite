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
package io.taskite.engine.processing.common;

import io.taskite.engine.state.immutable.*;
import io.taskite.protocol.impl.record.value.broker.JobRecord;
import java.util.Collection;
import java.util.List;

/** Job-local incident and variable resolution; no workflow ownership is required. */
public final class JobActivationBehavior {
  private final IncidentState incidents;
  private final VariableState variables;

  public JobActivationBehavior(final ProcessingState state) {
    incidents = state.getIncidentState();
    variables = state.getVariableState();
  }

  public boolean canActivate(final long key, final JobRecord job) {
    return job.getRetries() > 0
        && incidents.getJobIncidentKey(key) == IncidentState.MISSING_INCIDENT;
  }

  public JobRecord withVariables(final long key, final JobRecord job) {
    return withVariables(key, job, List.of());
  }

  public JobRecord withVariables(
      final long key, final JobRecord job, final Collection<String> names) {
    return job.copy()
        .setVariablesMap(
            names.isEmpty() ? variables.getVariables(key) : variables.getVariables(key, names));
  }
}
