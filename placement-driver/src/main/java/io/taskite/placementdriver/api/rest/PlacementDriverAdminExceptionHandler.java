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
package io.taskite.placementdriver.api.rest;

import io.taskite.placementdriver.api.model.PlacementDriverHttpModel.AdminOperationErrorResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.NoHandlerFoundException;
import org.springframework.web.servlet.resource.NoResourceFoundException;

/**
 * Preserves the previous hand-rolled router's error envelopes for requests that never reach a
 * handler: an unknown path returns {@code 404 NOT_FOUND} and an unsupported method returns {@code
 * 405 METHOD_NOT_ALLOWED}, both as {@link AdminOperationErrorResponse} JSON (byte-for-byte
 * compatible with the pre-Spring behavior). Requires {@code
 * spring.mvc.throw-exception-if-no-handler-found=true} and static-resource mapping disabled so
 * these exceptions are actually raised.
 */
@RestControllerAdvice
public class PlacementDriverAdminExceptionHandler {

  @ExceptionHandler({NoHandlerFoundException.class, NoResourceFoundException.class})
  public ResponseEntity<AdminOperationErrorResponse> handleNotFound(final Exception exception) {
    return ResponseEntity.status(HttpStatus.NOT_FOUND)
        .body(new AdminOperationErrorResponse("NOT_FOUND", exception.getMessage(), null));
  }

  @ExceptionHandler(HttpRequestMethodNotSupportedException.class)
  public ResponseEntity<AdminOperationErrorResponse> handleMethodNotAllowed(
      final HttpRequestMethodNotSupportedException exception) {
    return ResponseEntity.status(HttpStatus.METHOD_NOT_ALLOWED)
        .body(new AdminOperationErrorResponse("METHOD_NOT_ALLOWED", exception.getMessage(), null));
  }
}
