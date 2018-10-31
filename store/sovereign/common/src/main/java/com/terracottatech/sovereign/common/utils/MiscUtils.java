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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Truly miscellaneous collection of stuff.
 *
 * @author cschanck
 */
public final class MiscUtils {

  private static final String[] UNITS = new String[] { " bytes", "K", "M", "G", "T", "P", "E" };

  private MiscUtils() {
  }

  public static String bytesAsNiceString(long bytes) {
    for (int i = 6; i > 0; i--) {
      double step = Math.pow(1024, i);
      if (bytes > step) {
        return String.format("%3.1f%s", bytes / step, UNITS[i]);
      }
    }
    return Long.toString(bytes) + UNITS[0];
  }

  public static int crappierStirhash(int h) {
    h ^= (h >>> 20) ^ (h >>> 12);
    return h ^ (h >>> 7) ^ (h >>> 4);
  }

  public static int hash32shiftmult(int key) {
    int c2 = 0x27d4eb2d; // a prime or an odd constant
    key = (key ^ 61) ^ (key >>> 16);
    key = key + (key << 3);
    key = key ^ (key >>> 4);
    key = key * c2;
    key = key ^ (key >>> 15);
    return key;
  }

  public static String randomString(Random r, int len) {
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < len; i++) {
      sb.append((char) ('A' + r.nextInt(25)));
    }
    return sb.toString();
  }

  public static long nextPowerOfTwo(int i) {
    long tmp = Integer.highestOneBit(i);
    if (tmp != i) {
      tmp = tmp << 1;
    }
    return tmp;
  }

  public static final void casLock(AtomicBoolean bool) {
    while (true) {
      if (bool.compareAndSet(false, true)) {
        return;
      }
    }
  }

  public static final void casUnlock(AtomicBoolean bool) {
    if (!bool.compareAndSet(true, false)) {
      throw new IllegalMonitorStateException();
    }
  }

  /**
   * The type Byte array container.
   *
   * @author cschanck
   */
  public static final class ByteArrayContainer {
    private final byte[] array;

    /**
     * Instantiates a new Byte array container.
     *
     * @param array the array
     */
    public ByteArrayContainer(final byte[] array) {
      this.array = Arrays.copyOf(array, array.length);
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public byte[] getArray() {
      return array;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      ByteArrayContainer container = (ByteArrayContainer) o;

      if (!Arrays.equals(array, container.array)) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return array != null ? Arrays.hashCode(array) : 0;
    }
  }

  public static long maskForBitsize(int bitsize) {
    if (bitsize == 0) {
      return 0;
    }
    long mask = 1;
    for (int i = 0; i < bitsize - 1; i++) {
      mask = mask << 1 | 1;
    }
    return mask;
  }

  public static int stirHash(int h) {
    return hash32shiftmult(h);
  }

  public static Boolean getBoolean(String propName, boolean defValue) {
    String val = System.getProperty(propName, Boolean.toString(defValue));
    return Boolean.valueOf(val);
  }
}
