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
package io.taskite.gateway.interceptor;

import io.grpc.ServerInterceptor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Loads configured interceptors and owns their lifecycle and external class loaders. */
public final class GatewayInterceptorRepository implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(GatewayInterceptorRepository.class);

  private final List<LoadedInterceptor> loaded = new ArrayList<>();
  private final List<ServerInterceptor> interceptors;
  private boolean closed;

  public GatewayInterceptorRepository(final List<GatewayInterceptorConfig> configs) {
    final List<GatewayInterceptorConfig> normalized =
        List.copyOf(configs == null ? List.of() : configs);
    validate(normalized);
    try {
      for (final GatewayInterceptorConfig config : normalized) {
        loaded.add(load(config));
      }
    } catch (final RuntimeException error) {
      closeAfterFailure(error);
      throw error;
    }
    interceptors = loaded.stream().map(LoadedInterceptor::exposed).toList();
  }

  public List<ServerInterceptor> interceptors() {
    return interceptors;
  }

  private static void validate(final List<GatewayInterceptorConfig> configs) {
    final Set<String> ids = new HashSet<>();
    for (final GatewayInterceptorConfig config : configs) {
      if (config.className().isBlank()) {
        throw new GatewayInterceptorLoadException("interceptor className must be present");
      }
      if (!ids.add(config.resolvedId())) {
        throw new GatewayInterceptorLoadException(
            "duplicate interceptor id: " + config.resolvedId());
      }
    }
  }

  private static LoadedInterceptor load(final GatewayInterceptorConfig config) {
    GatewayInterceptorClassLoader externalLoader = null;
    try {
      final ClassLoader loader;
      final String source;
      if (config.jarPath().isBlank()) {
        loader = GatewayInterceptorRepository.class.getClassLoader();
        source = "application classpath";
      } else {
        final Path jar = Path.of(config.jarPath()).toAbsolutePath().normalize();
        if (!Files.isRegularFile(jar)) {
          throw new GatewayInterceptorLoadException("interceptor JAR does not exist: " + jar);
        }
        externalLoader =
            new GatewayInterceptorClassLoader(
                jar.toUri().toURL(), GatewayInterceptorRepository.class.getClassLoader());
        loader = externalLoader;
        source = jar.toString();
      }

      final Class<? extends ServerInterceptor> interceptorType =
          Class.forName(config.className(), true, loader).asSubclass(ServerInterceptor.class);
      final ServerInterceptor delegate = interceptorType.getConstructor().newInstance();
      final ServerInterceptor exposed =
          externalLoader == null
              ? delegate
              : new ContextClassLoaderInterceptor(delegate, externalLoader);
      LOG.info(
          "loaded Gateway interceptor id={}, class={}, source={}",
          config.resolvedId(),
          config.className(),
          source);
      return new LoadedInterceptor(delegate, exposed, externalLoader);
    } catch (final GatewayInterceptorLoadException error) {
      closeLoader(externalLoader, error);
      throw error;
    } catch (final ClassCastException error) {
      closeLoader(externalLoader, error);
      throw new GatewayInterceptorLoadException(
          "configured class does not implement ServerInterceptor: " + config.className(), error);
    } catch (final NoSuchMethodException error) {
      closeLoader(externalLoader, error);
      throw new GatewayInterceptorLoadException(
          "interceptor must declare a public no-argument constructor: " + config.className(),
          error);
    } catch (final InvocationTargetException error) {
      closeLoader(externalLoader, error);
      throw new GatewayInterceptorLoadException(
          "interceptor constructor failed: " + config.className(), error.getCause());
    } catch (final Exception | LinkageError error) {
      closeLoader(externalLoader, error);
      throw new GatewayInterceptorLoadException(
          "failed to load interceptor " + config.className(), error);
    }
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    RuntimeException failure = null;
    for (int index = loaded.size() - 1; index >= 0; index--) {
      final LoadedInterceptor interceptor = loaded.get(index);
      try {
        interceptor.close();
      } catch (final Exception error) {
        if (failure == null) {
          failure =
              new GatewayInterceptorLoadException("failed to close Gateway interceptor", error);
        } else {
          failure.addSuppressed(error);
        }
      }
    }
    loaded.clear();
    if (failure != null) {
      throw failure;
    }
  }

  private void closeAfterFailure(final RuntimeException startupFailure) {
    try {
      close();
    } catch (final RuntimeException closeFailure) {
      startupFailure.addSuppressed(closeFailure);
    }
  }

  private static void closeLoader(
      final GatewayInterceptorClassLoader loader, final Throwable failure) {
    if (loader == null) {
      return;
    }
    try {
      loader.close();
    } catch (final Exception closeFailure) {
      failure.addSuppressed(closeFailure);
    }
  }

  private record LoadedInterceptor(
      ServerInterceptor delegate,
      ServerInterceptor exposed,
      GatewayInterceptorClassLoader classLoader) {
    private void close() throws Exception {
      Exception failure = null;
      if (delegate instanceof AutoCloseable closeable) {
        try {
          closeable.close();
        } catch (final Exception error) {
          failure = error;
        }
      }
      if (classLoader != null) {
        try {
          classLoader.close();
        } catch (final Exception error) {
          if (failure == null) {
            failure = error;
          } else {
            failure.addSuppressed(error);
          }
        }
      }
      if (failure != null) {
        throw failure;
      }
    }
  }
}
