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
package io.taskite.streamplatform.command;

public interface CommandFailureObserver {
  void onFailure(String requestId, Throwable error);

  static CommandFailureObserver noop() {
    return new CommandFailureObserver() {
      @Override
      public void onFailure(final String requestId, final Throwable error) {}
    };
  }
}
