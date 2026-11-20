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
package io.taskite.topology;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.io.IOException;
import java.util.Objects;

/** An observation of stable, applied Raft configuration, independent of PD's placement target. */
@JsonSerialize(using = BrokerPartitionElectionState.Serializer.class)
@JsonDeserialize(using = BrokerPartitionElectionState.Deserializer.class)
public record BrokerPartitionElectionState(
    RaftConfigurationReference configuration, Priorities priorities, Observation observation) {
  public static final BrokerPartitionElectionState UNKNOWN =
      new BrokerPartitionElectionState(
          RaftConfigurationReference.UNKNOWN,
          new Priorities(-1, -1),
          new Observation(RaftNodeState.UNKNOWN, "", false, 0));

  public BrokerPartitionElectionState {
    Objects.requireNonNull(configuration, "configuration");
    Objects.requireNonNull(priorities, "priorities");
    Objects.requireNonNull(observation, "observation");
  }

  public long configurationIndex() {
    return configuration.index();
  }

  public String configurationDigest() {
    return configuration.digest();
  }

  public int desiredPriority() {
    return priorities.desired();
  }

  public int targetPriority() {
    return priorities.target();
  }

  public RaftNodeState nodeState() {
    return observation.nodeState();
  }

  public String leaderId() {
    return observation.leaderId();
  }

  public boolean serviceReady() {
    return observation.serviceReady();
  }

  public long observedAtEpochMillis() {
    return observation.observedAtEpochMillis();
  }

  public record Priorities(int desired, int target) {}

  public record Observation(
      RaftNodeState nodeState, String leaderId, boolean serviceReady, long observedAtEpochMillis) {
    public Observation {
      nodeState = nodeState == null ? RaftNodeState.UNKNOWN : nodeState;
      leaderId = leaderId == null ? "" : leaderId;
    }
  }

  /**
   * Preserves the public flat observation contract independently of the internal parameter groups.
   */
  public static final class Serializer extends JsonSerializer<BrokerPartitionElectionState> {
    @Override
    public void serialize(
        final BrokerPartitionElectionState value,
        final JsonGenerator json,
        final SerializerProvider provider)
        throws IOException {
      json.writeStartObject();
      json.writeNumberField("configurationIndex", value.configurationIndex());
      json.writeStringField("configurationDigest", value.configurationDigest());
      json.writeNumberField("desiredPriority", value.desiredPriority());
      json.writeNumberField("targetPriority", value.targetPriority());
      json.writeStringField("nodeState", value.nodeState().name());
      json.writeStringField("leaderId", value.leaderId());
      json.writeBooleanField("serviceReady", value.serviceReady());
      json.writeNumberField("observedAtEpochMillis", value.observedAtEpochMillis());
      json.writeEndObject();
    }
  }

  public static final class Deserializer extends JsonDeserializer<BrokerPartitionElectionState> {
    @Override
    public BrokerPartitionElectionState deserialize(
        final JsonParser parser, final DeserializationContext context) throws IOException {
      final JsonNode json = parser.getCodec().readTree(parser);
      return new BrokerPartitionElectionState(
          new RaftConfigurationReference(
              json.path("configurationIndex").asLong(),
              json.path("configurationDigest").asText("")),
          new Priorities(
              json.path("desiredPriority").asInt(-1), json.path("targetPriority").asInt(-1)),
          new Observation(
              RaftNodeState.parse(json.path("nodeState").asText()),
              json.path("leaderId").asText(""),
              json.path("serviceReady").asBoolean(),
              json.path("observedAtEpochMillis").asLong()));
    }
  }
}
