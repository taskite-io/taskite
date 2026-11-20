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
package io.taskite.dist;

import io.taskite.placementdriver.PlacementDriverApplication;

/**
 * Thin launcher that validates complete Spring Boot property arguments and hands off to {@link
 * PlacementDriverApplication}. Spring Boot owns the process lifecycle (embedded Tomcat + graceful
 * shutdown), so no manual shutdown hook or block-until-shutdown is needed here.
 */
public final class StandalonePlacementDriver {
  private StandalonePlacementDriver() {}

  public static void main(final String[] args) {
    PlacementDriverApplication.main(translate(args));
  }

  static String[] translate(final String[] args) {
    return ConfigurationArguments.validate(args);
  }
}
