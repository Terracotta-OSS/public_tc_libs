/*
 * Copyright (c) 2012-2018 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.terracottatech.sovereign.btrees.stores.disk;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * The interface Block buffer. Used for either mapped or unmapped blocks on disk.
 */
public interface BlockBuffer {

  /**
   * Gets id.
   *
   * @return the id
   */
  int getId();

  /**
   * Fetch an int from a position.
   * @param offset
   * @return
   * @throws IOException
   */
  int readInt(int offset) throws IOException;

  /**
   * Read a buffer from a position.
   *
   * @param offset
   * @param dest
   * @throws IOException
   */
  void read(int offset, ByteBuffer dest) throws IOException;

  /**
   * Read only; just grab a slice.
   *
   * @param offset
   * @param many
   * @return
   * @throws IOException
   */
  ByteBuffer readOnly(int offset, int many) throws IOException;

  /**
   * Write an int at a position.
   *
   * @param offset
   * @param value
   * @throws IOException
   */
  void writeInt(int offset, int value) throws IOException;

  /**
   * Write a buffer.
   *
   * @param offset
   * @param src
   * @throws IOException
   */
  void write(int offset, ByteBuffer src) throws IOException;

  /**
   * Gets base address.
   *
   * @return the base address
   */
  public int getBaseAddress();

  /**
   * Flush to disk.
   *
   * @return
   * @throws IOException
   */
  public boolean flush() throws IOException;

  /**
   * Close.
   *
   * @throws IOException
   */
  void close() throws IOException;

  /**
   * Gets free pages.
   *
   * @return the free pages
   */
  int getFreePages();

  /**
   * Free an offset (location).
   * @param offset
   */
  void free(int offset);

  /**
   * Allocate a location, based on the page size.
   */
  int alloc();

  /**
   * Reset allocations.
   *
   * @param max
   */
  void initAllocations(int max);

}
