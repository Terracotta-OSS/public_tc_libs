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
 * Simple store reader interface.
 */
public interface SimpleStoreReader {

  /**
   * Read from a location.
   *
   * @param offset
   * @param buf
   * @throws IOException
   */
  public void read(long offset, ByteBuffer buf) throws IOException;

  /**
   * Read only from a location.
   *
   * @param addr
   * @param many
   * @return
   * @throws IOException
   */
  public ByteBuffer readOnly(long addr, int many) throws IOException;

  /**
   * Gets commit data.
   *
   * @param buf the buf
   * @throws IOException the iO exception
   */
  public void getCommitData(ByteBuffer buf) throws IOException;

  /**
   * Close the reader.
   * @throws IOException
   */
  public void close() throws IOException;

  /**
   * Does this store support read only.
   *
   * @return
   */
  public boolean supportsReadOnly();

  /**
   * Gets page size.
   *
   * @return the page size
   */
  public int getPageSize();

  /**
   * Is this store new (empty).
   * @return
   */
  public boolean isNew();
}
