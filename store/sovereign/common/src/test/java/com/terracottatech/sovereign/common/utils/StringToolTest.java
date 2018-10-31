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

import org.junit.Test;

import java.io.UTFDataFormatException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

/**
 * Tests methods in {@link StringTool}.
 *
 * @author Clifford W. Johnson
 */
public class StringToolTest {

  @Test
  public void testNulls() throws Exception {
    final String string = null;

    try {
      StringTool.getLengthAsUTF(string);
      fail();
    } catch (NullPointerException e) {
      // expected
    }

    try {
      final ByteBuffer buffer = ByteBuffer.allocate(4096);
      StringTool.putUTF(buffer, string);
      fail();
    } catch (NullPointerException e) {
      // expected
    }

    try {
      StringTool.putUTF(null, "");
      fail();
    } catch (NullPointerException e) {
      // expected
    }

    try {
      StringTool.getUTF(null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }

  }

  @Test
  public void testEmpty() throws Exception {
    testString("", 3, ByteBuffer.allocate(4096));
  }

  @Test
  public void test0x00() throws Exception {
    testString("\u0000", 5, ByteBuffer.allocate(4096));
  }

  @Test
  public void testMaxSmallAscii() throws Exception {
    char[] chars = new char[65535];
    Arrays.fill(chars, 'a');
    final ByteBuffer buffer = ByteBuffer.allocate(65 * 1024);
    testString(String.valueOf(chars), 3 + chars.length, buffer);
  }

  @Test
  public void testLargeAscii() throws Exception {
    char[] chars = new char[65536];
    Arrays.fill(chars, 'a');
    final ByteBuffer buffer = ByteBuffer.allocate(65 * 1024);
    testString(String.valueOf(chars), 5 + chars.length, buffer);
  }

  @Test
  public void testLargeNonAscii() throws Exception {
    char[] chars = new char[65536];
    Arrays.fill(chars, '\u00A2');
    final ByteBuffer buffer = ByteBuffer.allocate(9 + chars.length * 2);
    testString(String.valueOf(chars), 9 + chars.length * 2, buffer);
  }

  @Test
  public void testLargeMixed() throws Exception {
    char[] chars = new char[65536];
    int expectedLength = 9;
    for (int i = 0; i < chars.length; i += 2) {
      chars[i] = 'a';
      chars[i + 1] = '\u00A2';
      expectedLength += 3;
    }
    final ByteBuffer buffer = ByteBuffer.allocate(expectedLength);
    testString(String.valueOf(chars), expectedLength, buffer);
  }

  @Test
  public void testLargerMixed() throws Exception {
    char[] chars = new char[65536 * 2];
    int expectedLength = 9;
    for (int i = 0; i < chars.length; i += 2) {
      chars[i] = 'a';
      chars[i + 1] = '\u00A2';
      expectedLength += 3;
    }
    final ByteBuffer buffer = ByteBuffer.allocate(expectedLength);
    testString(String.valueOf(chars), expectedLength, buffer);
  }

  @Test
  public void testAllAscii() throws Exception {
    final ByteBuffer buffer = ByteBuffer.allocate(4096);
    for (char c = '\u0001'; c < '\u0080'; c++) {
      testString(String.valueOf(c), 4, buffer);
      buffer.clear();
    }
  }

  @Test
  public void testAllNonAscii() throws Exception {
    final ByteBuffer buffer = ByteBuffer.allocate(4096);
    for (char c = '\u0080'; c < '\u0800'; c++) {
      testString(String.valueOf(c), 5, buffer);
      buffer.clear();
    }
  }

  @Test
  public void testAllExtended() throws Exception {
    final ByteBuffer buffer = ByteBuffer.allocate(4096);
    for (char c = '\u0800'; c != 0; c++) {
      testString(String.valueOf(c), 6, buffer);
      buffer.clear();
    }
  }

  @Test
  public void testAscii() throws Exception {
    final StringBuilder sb = new StringBuilder(0x007F);
    for (char c = '\u0001'; c < '\u0080'; c++) {
      sb.append(c);
    }
    testString(sb.toString(), 3 + sb.length(), ByteBuffer.allocate(4096));
  }

  @Test
  public void testNonAscii() throws Exception {
    final StringBuilder sb = new StringBuilder(0x0800 - 0x0080);
    for (char c = '\u0080'; c < '\u0800'; c++) {
      sb.append(c);
    }
    testString(sb.toString(), 3 + (sb.length() * 2), ByteBuffer.allocate(4096));
  }

  @Test
  public void testSmallExtended() throws Exception {
    final int maxCount = 65535 / 3;
    final StringBuilder sb = new StringBuilder(maxCount);
    for (char c = '\u0800'; c < '\u0800' + maxCount; c++) {
      sb.append(c);
    }
    testString(sb.toString(), 3 + (sb.length() * 3), ByteBuffer.allocate(47 * 4096));
  }

  @Test
  public void testLargeExtended() throws Exception {
    final StringBuilder sb = new StringBuilder(0x10000 - 0x0800);
    for (char c = '\u0800'; c != 0; c++) {
      sb.append(c);
    }
    final int expectedLength = 9 + (sb.length() * 2);
    testString(sb.toString(), expectedLength, ByteBuffer.allocate(expectedLength));
  }

  @Test
  public void testShortAsciiAscii() throws Exception {
    testString("66", 3 + 2, ByteBuffer.allocate(3 + 2));
  }

  @Test
  public void testShortAscii2Byte() throws Exception {
    testString("6\u05B0", 3 + 3, ByteBuffer.allocate(3 + 3));
  }

  @Test
  public void testShortAscii3Byte() throws Exception {
    testString("6\u20A5", 3 + 4, ByteBuffer.allocate(3 + 4));
  }

  @Test
  public void testShort2Byte2Byte() throws Exception {
    testString("\u05B0\u05B0", 3 + 4, ByteBuffer.allocate(3 + 4));
  }

  @Test
  public void testShort2ByteAscii() throws Exception {
    testString("\u05B06", 3 + 3, ByteBuffer.allocate(3 + 3));
  }

  @Test
  public void test2Byte3Byte() throws Exception {
    testString("\u05B0\u20A5", 3 + 5, ByteBuffer.allocate(3 + 5));
  }

  @Test
  public void testShort3Byte3Byte() throws Exception {
    testString("\u20A5\u20A5", 3 + 6, ByteBuffer.allocate(3 + 6));
  }

  @Test
  public void testShort3ByteAscii() throws Exception {
    testString("\u20A56", 3 + 4, ByteBuffer.allocate(3 + 4));
  }

  @Test
  public void test3Byte2Byte() throws Exception {
    testString("\u20A5\u05B0", 3 + 5, ByteBuffer.allocate(3 + 5));
  }

  private void testString(final String string, final int expectedLength, final ByteBuffer buffer)
      throws UTFDataFormatException {
    assertThat(StringTool.getLengthAsUTF(string), is(expectedLength));
    StringTool.putUTF(buffer, string);
    assertThat(buffer.position(), is(expectedLength));
    buffer.rewind();
    assertThat(StringTool.getUTF(buffer), is(string));
  }
}
