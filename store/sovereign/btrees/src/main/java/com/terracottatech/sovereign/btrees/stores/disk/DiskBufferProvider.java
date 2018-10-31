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
import com.terracottatech.sovereign.common.utils.FileUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static com.terracottatech.sovereign.common.utils.FileUtils.writeFully;

/**
 * Disk buffer provider. Creates a buffer, mapped or unmapped, perhaps.
 */
public interface DiskBufferProvider {

  /**
   * Provision (create) a buffer from a buffer source location.
   *
   * @param bsl
   * @param channel
   * @param offset
   * @param length
   * @return
   * @throws IOException
   */
  ByteBuffer provisionBuffer(PageSourceLocation bsl, FileChannel channel, int offset, int length) throws
    IOException;

  /**
   * Provision (create) a buffer mapping when the buffer is provided.
   * @param buf
   * @param channel
   * @param offset
   * @param length
   * @throws IOException
   */
  void provisionBuffer(ByteBuffer buf, FileChannel channel, long offset, int length) throws IOException;

  /**
   * Flush a buffer to a location.
   *
   * @param channel
   * @param buf
   * @param offset
   * @throws IOException
   */
  void flush(FileChannel channel, ByteBuffer buf, long offset) throws IOException;

  /**
   * The type Unmapped.
   *
   * @author cschanck
   */
  public static class Unmapped implements DiskBufferProvider {

    @Override
    public ByteBuffer provisionBuffer(PageSourceLocation bsl, FileChannel channel, int offset,
                                      int length) throws IOException {
      ByteBuffer b = bsl.allocateBuffer(length);
      b.clear();
      FileUtils.readFully(channel, b, offset);
      return b;
    }

    @Override
    public void provisionBuffer(ByteBuffer buf, FileChannel channel, long offset, int length) throws IOException {
      buf.clear();
      buf.clear();
      FileUtils.readFully(channel, buf, offset);
    }

    @Override
    public void flush(FileChannel channel, ByteBuffer buf, long offset) throws IOException {
      buf.clear();
      writeFully(channel, buf, offset);
      buf.clear();
    }
  }

  /**
   * The type Mapped.
   *
   * @author cschanck
   */
  public static class Mapped implements DiskBufferProvider {

    @Override
    public ByteBuffer provisionBuffer(PageSourceLocation bsl, FileChannel channel, int offset,
                                      int length) throws IOException {
      MappedByteBuffer b = channel.map(FileChannel.MapMode.READ_WRITE, offset, length);
      b.clear();
      return b;
    }

    @Override
    public void provisionBuffer(ByteBuffer buf, FileChannel channel, long offset, int length) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void flush(FileChannel channel, ByteBuffer buf, long offset) throws IOException {
      ((MappedByteBuffer) buf).force();
    }
  }

}
