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
package com.terracottatech.sovereign.btrees.stores;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Simple store writer.
 */
public interface SimpleStoreWriter {

  /**
   * Append buffers of data to the store.
   *
   * @param bufs
   * @return
   * @throws IOException
   */
  long append(ByteBuffer... bufs) throws IOException;

  /**
   * Discard any in-flight writes.
   *
   * @throws IOException
   */
  void discard() throws IOException;

  /**
   * Close.
   *
   * @throws IOException
   */
  void close() throws IOException;

  /**
   * Store a location as freed, possibly for right now, alternatively, on commit.
   *
   * @param addr
   * @param immediate
   * @throws IOException
   */
  void storeFree(long addr, boolean immediate) throws IOException;

  /**
   * Is it durable.
   *
   * @return
   */
  boolean supportsDurability();
}

