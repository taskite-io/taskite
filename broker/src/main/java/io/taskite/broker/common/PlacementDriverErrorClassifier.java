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
package io.taskite.broker.common;

import io.grpc.Status;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

public final class PlacementDriverErrorClassifier {
  private PlacementDriverErrorClassifier() {}

  public static boolean isBackpressure(final Throwable error) {
    if (error == null) {
      return false;
    }
    final Status status = Status.fromThrowable(unwrap(error));
    return status.getCode() == Status.Code.RESOURCE_EXHAUSTED
        && status.getDescription() != null
        && status.getDescription().startsWith("BACKPRESSURE:");
  }

  public static String errorMessage(final Throwable error) {
    final Throwable root = unwrap(error);
    final String message = root.getMessage();
    return message == null || message.isBlank()
        ? root.getClass().getSimpleName()
        : root.getClass().getSimpleName() + ": " + message;
  }

  public static Throwable unwrap(final Throwable error) {
    Throwable current = error;
    while ((current instanceof CompletionException || current instanceof ExecutionException)
        && current.getCause() != null) {
      current = current.getCause();
    }
    return current;
  }
}
