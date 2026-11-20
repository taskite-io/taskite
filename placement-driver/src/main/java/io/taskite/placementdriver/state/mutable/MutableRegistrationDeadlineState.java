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
package io.taskite.placementdriver.state.mutable;

import io.taskite.placementdriver.broker.registry.BrokerRegistration;
import io.taskite.placementdriver.state.immutable.RegistrationDeadlineState;
import io.taskite.placementdriver.worker.registry.WorkerRegistration;
import java.util.List;

public interface MutableRegistrationDeadlineState extends RegistrationDeadlineState {
  void updateBroker(BrokerRegistration previous, BrokerRegistration current);

  void removeBroker(BrokerRegistration registration);

  void updateWorker(WorkerRegistration previous, WorkerRegistration current);

  void removeWorker(WorkerRegistration registration);

  void rebuild(
      List<BrokerRegistration> brokerRegistrations, List<WorkerRegistration> workerRegistrations);
}
