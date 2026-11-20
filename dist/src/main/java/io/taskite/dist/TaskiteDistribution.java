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

import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TaskiteDistribution {
  private static final Logger LOG = LoggerFactory.getLogger(TaskiteDistribution.class);

  private TaskiteDistribution() {}

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      usage();
      return;
    }
    String command = args[0];
    String[] remaining = Arrays.copyOfRange(args, 1, args.length);
    switch (command) {
      case "broker" -> StandaloneBroker.main(remaining);
      case "gateway" -> StandaloneGateway.main(remaining);
      case "placement-driver", "placementDriver", "pd" -> StandalonePlacementDriver.main(remaining);
      default -> {
        usage();
        throw new IllegalArgumentException("unknown Taskite Server command: " + command);
      }
    }
  }

  private static void usage() {
    LOG.info(
        "usage: java -jar taskite-distribution.jar <broker|gateway|placement-driver> --key=value ...");
  }
}
