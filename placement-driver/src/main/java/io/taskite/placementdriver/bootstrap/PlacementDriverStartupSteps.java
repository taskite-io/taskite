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
package io.taskite.placementdriver.bootstrap;

import java.util.ArrayList;
import java.util.List;

public final class PlacementDriverStartupSteps {
  private PlacementDriverStartupSteps() {}

  public static List<PlacementDriverStartupStep<PlacementDriverStartupContext>> build() {
    final List<PlacementDriverStartupStep<PlacementDriverStartupContext>> steps = new ArrayList<>();
    steps.add(new PlacementDriverConfigurationStep());
    steps.add(new PlacementDriverProcessorStep());
    steps.add(new PlacementDriverTopologyManifestRestoreStep());
    steps.add(new PlacementDriverApiStep());
    steps.add(new PlacementDriverBootstrapMembershipStep());
    steps.add(new PlacementDriverRaftStep());
    steps.add(new PlacementDriverRuntimeComponentsStep());
    steps.add(new PlacementDriverHealthCheckStep());
    return steps;
  }
}
