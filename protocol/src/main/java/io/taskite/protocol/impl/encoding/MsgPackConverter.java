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
package io.taskite.protocol.impl.encoding;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.util.BufferUtil;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.agrona.DirectBuffer;
import org.msgpack.core.MessagePackException;
import org.msgpack.jackson.dataformat.MessagePackFactory;

/** Shared conversions between objects, JSON text and MessagePack documents. */
public final class MsgPackConverter {
  private static final MessagePackFactory MESSAGE_PACK_FACTORY =
      new MessagePackFactory().setReuseResourceInGenerator(false).setReuseResourceInParser(false);
  private static final ObjectMapper MAPPER =
      new ObjectMapper(MESSAGE_PACK_FACTORY)
          .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
  private static final JsonFactory JSON_FACTORY = new JsonFactory().setCodec(MAPPER);

  private static final ClassValue<List<Field>> RECORD_FIELDS =
      new ClassValue<>() {
        @Override
        protected List<Field> computeValue(final Class<?> type) {
          final List<Field> fields = new ArrayList<>();
          for (Class<?> current = type;
              current != null && current != UnifiedRecordValue.class;
              current = current.getSuperclass()) {
            for (final Field field : current.getDeclaredFields()) {
              if (Modifier.isStatic(field.getModifiers())
                  || Modifier.isTransient(field.getModifiers())) {
                continue;
              }
              if (Modifier.isFinal(field.getModifiers()) || !field.trySetAccessible()) {
                throw new IllegalArgumentException("Record field must be assignable: " + field);
              }
              fields.add(field);
            }
          }
          return List.copyOf(fields);
        }
      };

  private MsgPackConverter() {}

  /** Converts JSON text, including scalar roots, to a MessagePack document. */
  public static byte[] convertToMsgPack(final String json) {
    try (final var parser = JSON_FACTORY.createParser(json);
        final var output = new ByteArrayOutputStream();
        final var generator = MESSAGE_PACK_FACTORY.createGenerator(output)) {
      requireValue(parser.nextToken());
      generator.copyCurrentStructure(parser);
      if (parser.nextToken() != null) {
        throw new IllegalArgumentException("Trailing data in JSON document");
      }
      generator.flush();
      return output.toByteArray();
    } catch (final IOException | MessagePackException error) {
      throw new IllegalArgumentException("Cannot convert JSON to MessagePack", error);
    }
  }

  /** Serializes an object, array or scalar directly as MessagePack. */
  public static byte[] convertToMsgPack(final Object value) {
    if (value != null) {
      requireConcreteRecord(value.getClass());
    }
    try {
      return MAPPER.writeValueAsBytes(value);
    } catch (final IOException | MessagePackException error) {
      throw new IllegalArgumentException("Cannot serialize object as MessagePack", error);
    }
  }

  public static String convertToJson(final DirectBuffer buffer) {
    return convertToJson(BufferUtil.bufferAsArray(buffer));
  }

  public static String convertToJson(final byte[] messagePack) {
    try (final var parser = MESSAGE_PACK_FACTORY.createParser(messagePack);
        final var output = new StringWriter();
        final var generator = JSON_FACTORY.createGenerator(output)) {
      requireValue(parser.nextToken());
      generator.copyCurrentStructure(parser);
      requireCompleteMessagePack(parser.getCurrentLocation().getByteOffset(), messagePack.length);
      generator.flush();
      return output.toString();
    } catch (final IOException | MessagePackException error) {
      throw new IllegalArgumentException("Cannot convert MessagePack to JSON", error);
    }
  }

  /** Reads one MessagePack value, accepting legacy JSON objects on persistence reads. */
  public static <T> T convertToObject(final Class<T> type, final byte[] messagePack) {
    requireConcreteRecord(type);
    // The MessagePack positive fixint 123 is also the ASCII byte '{'.
    if (messagePack.length > 1 && isJsonObject(messagePack)) {
      return convertJsonToObject(new String(messagePack, StandardCharsets.UTF_8), type);
    }
    try (final var parser = MESSAGE_PACK_FACTORY.createParser(messagePack)) {
      final JsonToken token = parser.nextToken();
      requireValue(token);
      if ((UnifiedRecordValue.class.isAssignableFrom(type) || Map.class.isAssignableFrom(type))
          && token != JsonToken.START_OBJECT) {
        throw new IllegalArgumentException("An object must be a MessagePack map");
      }
      final T value = MAPPER.readValue(parser, type);
      requireCompleteMessagePack(parser.getCurrentLocation().getByteOffset(), messagePack.length);
      if (value == null && UnifiedRecordValue.class.isAssignableFrom(type)) {
        throw new IllegalArgumentException("Null object");
      }
      return value;
    } catch (final IOException | MessagePackException error) {
      throw new IllegalArgumentException("Cannot deserialize MessagePack as " + type, error);
    }
  }

  @SuppressWarnings("unchecked")
  public static Map<String, Object> convertToMap(final DirectBuffer buffer) {
    return convertToObject(Map.class, BufferUtil.bufferAsArray(buffer));
  }

  public static <T> T convertJsonToObject(final String json, final Class<T> type) {
    try (final var parser = JSON_FACTORY.createParser(json)) {
      return MAPPER
          .readerFor(type)
          .with(
              DeserializationFeature.FAIL_ON_TRAILING_TOKENS,
              DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
          .readValue(parser);
    } catch (final IOException error) {
      throw new IllegalArgumentException("Cannot deserialize JSON as " + type, error);
    }
  }

  @SuppressWarnings("unchecked")
  public static Map<String, Object> convertJsonToMap(final String json) {
    return convertJsonToObject(json, Map.class);
  }

  public static String convertObjectToJson(final Object value) {
    try (final var output = new StringWriter();
        final var generator = JSON_FACTORY.createGenerator(output)) {
      MAPPER.writeValue(generator, value);
      generator.flush();
      return output.toString();
    } catch (final IOException error) {
      throw new IllegalArgumentException("Cannot serialize object as JSON", error);
    }
  }

  @SuppressWarnings("unchecked")
  public static <T> T copy(final T value) {
    return (T) convertToObject(value.getClass(), convertToMsgPack(value));
  }

  /** Decodes the given window completely before replacing any fields of the reusable record. */
  public static void decodeInto(
      final UnifiedRecordValue target,
      final DirectBuffer buffer,
      final int offset,
      final int length) {
    buffer.boundsCheck(offset, length);
    if (length <= 0) {
      throw new IllegalArgumentException("Empty record value");
    }
    final byte[] bytes = new byte[length];
    buffer.getBytes(offset, bytes);
    final Object decoded = convertToObject(target.getClass(), bytes);
    final List<Field> fields = RECORD_FIELDS.get(target.getClass());
    try {
      for (final Field field : fields) {
        field.set(target, field.get(decoded));
      }
    } catch (final IllegalAccessException error) {
      throw new IllegalStateException("Cannot populate record value", error);
    }
  }

  private static void requireConcreteRecord(final Class<?> type) {
    if (type == UnifiedRecordValue.class) {
      throw new IllegalArgumentException("A concrete record value is required");
    }
  }

  private static boolean isJsonObject(final byte[] bytes) {
    for (final byte value : bytes) {
      if (value != ' ' && value != '\t' && value != '\n' && value != '\r') {
        return value == '{';
      }
    }
    return false;
  }

  private static void requireValue(final JsonToken token) {
    if (token == null || (!token.isStructStart() && !token.isScalarValue())) {
      throw new IllegalArgumentException("Document must contain an object, an array or a scalar");
    }
  }

  private static void requireCompleteMessagePack(final long consumed, final int length) {
    // msgpack-java 0.9.8 throws at EOF; do not call nextToken() to check for trailing data.
    if (consumed != length) {
      throw new IllegalArgumentException("Trailing bytes in MessagePack document");
    }
  }
}
