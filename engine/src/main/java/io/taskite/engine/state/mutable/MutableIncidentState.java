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
package io.taskite.engine.state.mutable;

import io.taskite.engine.state.immutable.IncidentState;
import io.taskite.protocol.impl.record.value.broker.IncidentRecord;

/** Transactional write access to incidents and their ownership indexes. */
public interface MutableIncidentState extends IncidentState {

  void createIncident(long incidentKey, IncidentRecord incident);

  void deleteIncident(long incidentKey);
}
