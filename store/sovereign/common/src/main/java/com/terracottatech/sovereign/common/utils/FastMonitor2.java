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

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Exists to record keys that are inserted into the btree as they happen. Fast buffered
 * writes and reads.
 *
 * @author cschanck
 **/
public class FastMonitor2 {

  public static final FastMonitor2 NOOP = new FastMonitor2() {

    @Override
    public final void writeOp(byte opCode, byte[] barr, int off, int len) {
      // noop
    }

    @Override
    public final void flush() throws IOException {
      // noop
    }

    @Override
    public void close() {
      // noop
    }
  };

  public static class Op {
    private final byte[] args;
    private final byte opCode;

    public Op(byte opCode, byte[] vals) {
      this.opCode = opCode;
      this.args = Arrays.copyOf(vals, vals.length);
    }

    public byte getOpCode() {
      return opCode;
    }

    public byte[] getArgs() {
      return Arrays.copyOf(args, args.length);
    }
  }

  private final Buffered buffered;
  private final DataOutputStream dos;

  private FastMonitor2() {
    buffered = null;
    dos = null;
  }

  public FastMonitor2(File f) throws IOException {
    this.buffered = new Buffered(f, 256 * 1024 * 1024);
    dos = new DataOutputStream(buffered);
  }

  public void writeOp(byte opCode, byte[] barr, int off, int len) {
    try {
      dos.writeByte(opCode);
      dos.writeShort(len);
      dos.write(barr, off, len);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static Op readOp(DataInputStream dis) throws IOException {
    byte opCode = dis.readByte();
    int arrlen = dis.readShort();
    if (arrlen < 0) {
      throw new IOException();
    }
    byte[] b = new byte[arrlen];
    int ret = dis.read(b);
    if (ret != b.length) {
      throw new IOException();
    }
    return new Op(opCode, b);
  }

  public void flush() throws IOException {
    dos.flush();
    buffered.flush();
  }

  public void close() {
    try {
      dos.close();
      buffered.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  static class Buffered extends OutputStream {
    private final FileOutputStream fos;
    private final BufferedOutputStream bufferStream;

    public Buffered(File f, int bufSize) throws IOException {
      if (f.exists()) {
        FileUtils.deleteRecursively(f);
      }
      this.fos = new FileOutputStream(f);
      bufferStream = new BufferedOutputStream(fos, bufSize);
    }

    @Override
    public void write(int b) throws IOException {
      bufferStream.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
      bufferStream.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      bufferStream.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
      bufferStream.flush();
    }

    public void close() throws IOException {
      bufferStream.flush();
      bufferStream.close();
      fos.close();
    }
  }

  public static class Reader implements Iterable<Op> {

    private final DataInputStream dis;

    public Reader(File f) throws FileNotFoundException {
      this(new FileInputStream(f));
    }

    public Reader(java.io.InputStream is) throws FileNotFoundException {
      dis = new DataInputStream(is);
    }

    @Override
    public Iterator<Op> iterator() {

      return new Iterator<Op>() {
        Op next = null;
        boolean dead = false;

        {
          advance();
        }

        private void advance() {
          if (next == null && !dead) {
            try {
              next = readOp(dis);
            } catch (IOException e) {
              dead = true;
              next = null;
            }
          }
        }

        @Override
        public boolean hasNext() {
          return next != null;
        }

        @Override
        public Op next() {
          if (hasNext()) {
            Op ret = next;
            next = null;
            advance();
            return ret;
          }
          throw new NoSuchElementException();
        }
      };
    }
  }

}
