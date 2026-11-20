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
package io.taskite.db.impl.rocksdb;

import static org.rocksdb.Status.Code.Aborted;
import static org.rocksdb.Status.Code.Busy;
import static org.rocksdb.Status.Code.Expired;
import static org.rocksdb.Status.Code.IOError;
import static org.rocksdb.Status.Code.MergeInProgress;
import static org.rocksdb.Status.Code.Ok;
import static org.rocksdb.Status.Code.TimedOut;
import static org.rocksdb.Status.Code.TryAgain;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.util.EnumSet;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksObject;
import org.rocksdb.Status;
import org.rocksdb.Status.Code;
import org.rocksdb.Transaction;

/**
 * Bridges to RocksDB JNI methods that the Java API does not expose publicly, so a {@link
 * org.rocksdb.Transaction} can put/get/delete against an arbitrary column-family handle by native
 * handle. Reflective method lookup is resolved once when the class is loaded.
 */
public final class RocksDbInternal {
  private RocksDbInternal() {}

  static final EnumSet<Code> RECOVERABLE_ERROR_CODES =
      EnumSet.of(Ok, Aborted, Expired, IOError, Busy, TimedOut, TryAgain, MergeInProgress);

  static Field nativeHandle;

  static MethodHandle putWithHandle;
  static MethodHandle getWithHandle;
  static MethodHandle removeWithHandle;

  static {
    RocksDB.loadLibrary();
    try {
      resolveInternalMethods();
    } catch (final Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private static void resolveInternalMethods() throws NoSuchFieldException, NoSuchMethodException {
    nativeHandles();
    putWithHandle();
    getWithHandle();
    removeWithHandle();
  }

  private static void nativeHandles() throws NoSuchFieldException {
    nativeHandle = RocksObject.class.getDeclaredField("nativeHandle_");
    nativeHandle.setAccessible(true);
  }

  private static void putWithHandle() throws NoSuchMethodException {
    final var method =
        Transaction.class.getDeclaredMethod(
            "put",
            Long.TYPE,
            byte[].class,
            Integer.TYPE,
            Integer.TYPE,
            byte[].class,
            Integer.TYPE,
            Integer.TYPE,
            Long.TYPE,
            Boolean.TYPE);
    method.setAccessible(true);
    try {
      putWithHandle = MethodHandles.lookup().unreflect(method);
    } catch (final IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  private static void getWithHandle() throws NoSuchMethodException {
    final var method =
        Transaction.class.getDeclaredMethod(
            "get", Long.TYPE, Long.TYPE, byte[].class, Integer.TYPE, Integer.TYPE, Long.TYPE);
    method.setAccessible(true);
    try {
      getWithHandle = MethodHandles.lookup().unreflect(method);
    } catch (final IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  private static void removeWithHandle() throws NoSuchMethodException {
    final var method =
        Transaction.class.getDeclaredMethod(
            "delete", Long.TYPE, byte[].class, Integer.TYPE, Long.TYPE, Boolean.TYPE);
    method.setAccessible(true);
    try {
      removeWithHandle = MethodHandles.lookup().unreflect(method);
    } catch (final IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  public static boolean isRocksDbExceptionRecoverable(final RocksDBException rdbex) {
    final Status status = rdbex.getStatus();
    return RECOVERABLE_ERROR_CODES.contains(status.getCode());
  }
}
