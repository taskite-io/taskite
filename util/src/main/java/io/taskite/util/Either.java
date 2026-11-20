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
package io.taskite.util;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A minimal right-biased value that contains either a {@link Left} or a {@link Right}. By
 * convention, left represents failure and right represents success.
 */
public sealed interface Either<L, R> permits Either.Left, Either.Right {

  public static <L, R> Either<L, R> left(final L value) {
    return new Left<>(value);
  }

  public static <L, R> Either<L, R> right(final R value) {
    return new Right<>(value);
  }

  boolean isLeft();

  boolean isRight();

  L getLeft();

  R get();

  <T> Either<L, T> map(Function<? super R, ? extends T> mapper);

  default <T> Either<L, T> flatMap(final Function<? super R, ? extends Either<L, T>> mapper) {
    return isLeft() ? Either.left(getLeft()) : mapper.apply(get());
  }

  default Either<L, R> thenDo(final Consumer<? super R> action) {
    if (isRight()) {
      action.accept(get());
    }
    return this;
  }

  record Left<L, R>(L value) implements Either<L, R> {
    public Left {
      Objects.requireNonNull(value, "value");
    }

    @Override
    public boolean isLeft() {
      return true;
    }

    @Override
    public boolean isRight() {
      return false;
    }

    @Override
    public L getLeft() {
      return value;
    }

    @Override
    public R get() {
      throw new IllegalStateException("Either is Left, not Right");
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Either<L, T> map(final Function<? super R, ? extends T> mapper) {
      return (Either<L, T>) this;
    }
  }

  record Right<L, R>(R value) implements Either<L, R> {
    @Override
    public boolean isLeft() {
      return false;
    }

    @Override
    public boolean isRight() {
      return true;
    }

    @Override
    public L getLeft() {
      throw new IllegalStateException("Either is Right, not Left");
    }

    @Override
    public R get() {
      return value;
    }

    @Override
    public <T> Either<L, T> map(final Function<? super R, ? extends T> mapper) {
      return Either.right(mapper.apply(value));
    }
  }
}
