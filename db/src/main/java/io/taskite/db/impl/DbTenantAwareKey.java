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
package io.taskite.db.impl;

import io.taskite.db.ContainsForeignKeys;
import io.taskite.db.DbKey;
import java.util.Collection;
import java.util.Collections;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * Wraps a key and prepends ({@link PlacementType#PREFIX}) or appends ({@link PlacementType#SUFFIX})
 * a tenant id to it. Use PREFIX to be able to scan by tenant; use SUFFIX to scan across tenants
 * without one tenant's keys sorting ahead of another's.
 */
public record DbTenantAwareKey<WrappedKey extends DbKey>(
    DbString tenantKey, WrappedKey wrappedKey, PlacementType placementType)
    implements DbKey, ContainsForeignKeys {

  @Override
  public void wrap(final DirectBuffer buffer, final int offset, final int length) {
    switch (placementType) {
      case PREFIX -> {
        tenantKey.wrap(buffer, offset, length);
        final var tenantKeyLength = tenantKey.getLength();
        wrappedKey.wrap(buffer, offset + tenantKeyLength, length - tenantKeyLength);
      }
      case SUFFIX -> {
        wrappedKey.wrap(buffer, offset, length);
        final var wrappedKeyLength = wrappedKey.getLength();
        tenantKey.wrap(buffer, offset + wrappedKeyLength, length - wrappedKeyLength);
      }
      default -> throw new IllegalStateException("Unexpected value: " + placementType);
    }
  }

  @Override
  public int getLength() {
    return wrappedKey.getLength() + tenantKey.getLength();
  }

  @Override
  public void write(final MutableDirectBuffer buffer, final int offset) {
    switch (placementType) {
      case PREFIX -> {
        tenantKey.write(buffer, offset);
        final var tenantKeyLength = tenantKey.getLength();
        wrappedKey.write(buffer, offset + tenantKeyLength);
      }
      case SUFFIX -> {
        wrappedKey.write(buffer, offset);
        final var wrappedKeyLength = wrappedKey.getLength();
        tenantKey.write(buffer, offset + wrappedKeyLength);
      }
      default -> throw new IllegalStateException("Unexpected value: " + placementType);
    }
  }

  @Override
  public Collection<DbForeignKey<DbKey>> containedForeignKeys() {
    if (wrappedKey instanceof ContainsForeignKeys foreignKeyProvider) {
      return foreignKeyProvider.containedForeignKeys();
    }
    return Collections.emptyList();
  }

  public enum PlacementType {
    PREFIX,
    SUFFIX
  }
}
