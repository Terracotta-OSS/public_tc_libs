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
package com.terracottatech.sovereign.common.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * Bunch a silly file utils.
 *
 * @author cschanck
 */
public final class FileUtils {
  private static final Logger log = LoggerFactory.getLogger(FileUtils.class);

  private FileUtils() {
  }

  public static boolean readFully(FileChannel fc, ByteBuffer b, long position) throws IOException {
    ByteBuffer tmp = b.slice();
    while (tmp.hasRemaining()) {
      int count = fc.read(tmp, position);
      if (count <= 0) {
        return false;
      }
      position = position + count;
    }
    return true;
  }

  public static void writeFully(FileChannel fc, ByteBuffer b, long position) throws IOException {
    while (b.hasRemaining()) {
      int cnt = fc.write(b, position);
      position = position + cnt;
    }
  }

  public static void deleteRecursively(File file) throws IOException {
    File[] contents = file.listFiles();
    if (contents != null) {
      for (File f : contents) {
        deleteRecursively(f);
      }
    }
    if (!file.delete() && file.exists()) {
      throw new IOException("Unable to delete: " + file);
    }
  }

  public static void touch(File f) throws IOException {
    if (!f.exists()) {
      new FileOutputStream(f).close();
    }
    boolean ret = f.setLastModified(System.currentTimeMillis());
    if (!ret) {
      throw new IOException();
    }
  }

  public static void truncate(File f) {
    try {
      new FileOutputStream(f, false).getChannel().truncate(0).close();
    } catch (IOException e) {
      log.info("Truncation error", e);
    }
  }

  /**
   * Gets int.
   *
   * @param fc  the fc
   * @param pos the pos
   * @return the int
   * @throws IOException the iO exception
   */
  public static int getInt(FileChannel fc, long pos) throws IOException {
    ByteBuffer b = ByteBuffer.wrap(new byte[4]);
    readFully(fc, b, pos);
    return b.getInt(0);
  }

  public static void putInt(FileChannel fc, long pos, int value) throws IOException {
    ByteBuffer b = ByteBuffer.wrap(new byte[4]);
    b.putInt(value);
    b.clear();
    writeFully(fc, b, pos);
  }

  public static ByteBuffer fetch(FileChannel src, long offset, int sz) throws IOException {
    ByteBuffer b = ByteBuffer.allocate(sz);
    b.clear();
    if (FileUtils.readFully(src, b, offset)) {
      b.clear();
      return b;
    }
    throw new IOException("Unable to read fully");
  }

  public static FileChannel fileChannel(File f) throws IOException {
    return FileChannel.open(Paths.get(f.getPath()), CREATE, READ, WRITE);
  }
}
