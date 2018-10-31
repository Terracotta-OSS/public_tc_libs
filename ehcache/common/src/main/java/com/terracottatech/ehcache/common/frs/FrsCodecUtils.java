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
package com.terracottatech.ehcache.common.frs;

import java.nio.ByteBuffer;

/**
 * Utility functions for use by different codecs
 *
 * @author RKAV
 */
public final class FrsCodecUtils {
  public static final int CHAR_SIZE = Character.SIZE / Byte.SIZE;
  public static final int LONG_SIZE = Long.SIZE / Byte.SIZE;
  public static final int INT_SIZE = Integer.SIZE / Byte.SIZE;
  public static final int FLOAT_SIZE = Float.SIZE / Byte.SIZE;
  public static final int DOUBLE_SIZE = Double.SIZE / Byte.SIZE;
  public static final int BYTE_SIZE = 1;

  public static void putString(final ByteBuffer bb, final String s) {
    final int len = s.length();
    bb.putInt(len);
    bb.asCharBuffer().put(s);
    bb.position(bb.position() + (len * CHAR_SIZE));
  }

  public static String extractString(final ByteBuffer bb) {
    final int len = bb.getInt();
    char[] ch = new char[len];
    for (int i = 0; i < len; i++) {
      ch[i] = bb.getChar();
    }
    return new String(ch);
  }
}