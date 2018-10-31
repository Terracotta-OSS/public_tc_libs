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


import com.terracottatech.sovereign.btrees.stores.location.PageSourceLocation;

import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * The interface for making block buffers with specific buffer source, block size, etc.
 */
public interface BlockBufferFactory {

  /**
   * Gets block size.
   *
   * @return the block size
   */
  int getBlockSize();

  /**
   * Gets buffer source location.
   *
   * @return the buffer source location
   */
  public PageSourceLocation getPageSourceLocation();

  /**
   * Gets disk provider.
   *
   * @return the disk provider
   */
  public DiskBufferProvider getDiskProvider();

  /**
   * Make one.
   *
   * @param id
   * @param channel
   * @param offset
   * @return
   * @throws IOException
   */
  BlockBuffer make(int id, FileChannel channel, int offset) throws IOException;

  /**
   * close the factory.
   *
   */
  void close();

}
