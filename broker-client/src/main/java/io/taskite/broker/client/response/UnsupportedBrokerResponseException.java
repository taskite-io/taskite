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
package io.taskite.broker.client.response;

public final class UnsupportedBrokerResponseException extends BrokerResponseException {

  private static final String SBE_SCHEMA_TEMPLATE_FORMAT =
      "Expected to receive message with schema id '%d' and template id '%d', but received schema id '%d' and template id '%d'";
  private static final String VALUE_TYPE_FORMAT =
      "Expected command response with value type '%s', but received '%s'";

  public UnsupportedBrokerResponseException(
      final int expectedSchemaId,
      final int expectedTemplateId,
      final int actualSchemaId,
      final int actualTemplateId) {
    super(
        String.format(
            SBE_SCHEMA_TEMPLATE_FORMAT,
            expectedSchemaId,
            expectedTemplateId,
            actualSchemaId,
            actualTemplateId));
  }

  public UnsupportedBrokerResponseException(
      final String expectedValueType, final String actualValueType) {
    super(String.format(VALUE_TYPE_FORMAT, expectedValueType, actualValueType));
  }
}
