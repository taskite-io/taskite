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

import org.agrona.MutableDirectBuffer;

/**
 * Implementations may add custom setters to specify values that should be written. Values are
 * written/copied when the {@link #write(MutableDirectBuffer, int)} method is called. Calling a
 * call-by-reference setter method (e.g. an Object setter) tells the writer <em>which object</em> to
 * write but not <em>what value</em>. The value is only determined at the time of writing, so that
 * value changes happening between setter and <em>#write</em> invocations affect the writer.
 */
public interface BufferWriter {
  /**
   * @return the number of bytes that this writer is going to write
   */
  int getLength();

  /**
   * Writes to a buffer.
   *
   * @param buffer the buffer that this writer writes to
   * @param offset the offset in the buffer that the writer begins writing at
   */
  void write(MutableDirectBuffer buffer, int offset);
}
