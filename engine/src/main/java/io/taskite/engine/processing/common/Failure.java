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

import io.taskite.protocol.record.value.broker.ErrorType;
import java.util.Objects;

/** Simple String wrapper for when something fails and a message needs to be used. */
public final class Failure {

  private final String message;
  private final ErrorType errorType;
  private final long variableScopeKey;

  public Failure(final String message) {
    this.message = message;
    errorType = null;
    variableScopeKey = -1L;
  }

  public Failure(final String message, final ErrorType errorType) {
    this.message = message;
    this.errorType = errorType;
    variableScopeKey = -1L;
  }

  public Failure(final String message, final ErrorType errorType, final long variableScopeKey) {
    this.message = message;
    this.errorType = errorType;
    this.variableScopeKey = variableScopeKey;
  }

  public String getMessage() {
    return message;
  }

  public ErrorType getErrorType() {
    return errorType;
  }

  public long getVariableScopeKey() {
    return variableScopeKey;
  }

  @Override
  public int hashCode() {
    return Objects.hash(message, errorType, variableScopeKey);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final Failure failure = (Failure) o;
    return variableScopeKey == failure.variableScopeKey
        && Objects.equals(message, failure.message)
        && errorType == failure.errorType;
  }

  @Override
  public String toString() {
    return "Failure{"
        + "message='"
        + message
        + '\''
        + ", errorType="
        + errorType
        + ", variableScopeKey="
        + variableScopeKey
        + '}';
  }
}
