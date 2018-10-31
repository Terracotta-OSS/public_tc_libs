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

package com.terracottatech.sovereign.testsupport;

import java.io.PrintStream;
import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.Arrays;
import java.util.Formatter;

import static java.util.Objects.requireNonNull;

/**
 * Dumps {@link Buffer} instances for diagnostic purposes.
 *
 * @author Clifford W. Johnson
 */
@SuppressWarnings("unused")
public class BufferDumper {

  /**
   * The {@code PrintStream} to which the dump is written.
   */
  private final PrintStream printStream;

  /**
   * Dumps the indicated buffer to {@code System.out}.
   *
   * @param buffer the {@code ByteBuffer} to dump
   */
  public static void dump(final ByteBuffer buffer) {
    new BufferDumper(System.out).dumpBuffer(buffer);
  }

  /**
   * Constructs a new {@code DumpUtility} using the {@code PrintStream} provided.
   *
   * @param printStream the {@code PrintStream} to which the dump is written
   */
  public BufferDumper(final PrintStream printStream) {
    requireNonNull(printStream, "printStream");
    this.printStream = printStream;
  }

  /**
   * Attempts to dump a {@link java.nio.IntBuffer IntBuffer}.  The present implementation of this
   * method depends the use of {@link java.nio.ByteBuffer#asIntBuffer() ByteBuffer.asIntBuffer} in
   * creation of {@code buffer} <b>and</b> on the internal implementation of {@code ByteBuffer}.
   *
   * @param buffer the {@code IntBuffer} instance to dump
   */
  public void dumpBuffer(final IntBuffer buffer) {

    final String bufferClassName = buffer.getClass().getName();
    final String byteBufferFieldName;
    switch (bufferClassName) {
      case "java.nio.ByteBufferAsInfBufferB":
      case "java.nio.ByteBufferAsIntBufferL":
      case "java.nio.ByteBufferAsIntBufferRB":
      case "java.nio.ByteBufferAsIntBufferRL":
        byteBufferFieldName = "bb";
        break;
      case "java.nio.DirectIntBufferU":
      case "java.nio.DirectIntBufferS":
      case "java.nio.DirectIntBufferRU":
      case "java.nio.DirectIntBufferRS":
        byteBufferFieldName = "att";
        break;
      default:
        throw new AssertionError(String.format("IntBuffer type not supported: %s", bufferClassName));
    }

    dumpBuffer(getFieldValue(ByteBuffer.class, buffer, byteBufferFieldName));
  }

  /**
   * Attempts to dump a {@link java.nio.LongBuffer LongBuffer}.  The present implementation of this
   * method depends the use of {@link java.nio.ByteBuffer#asLongBuffer() ByteBuffer.asLongBuffer} in
   * creation of {@code buffer} <b>and</b> on the internal implementation of {@code ByteBuffer}.
   *
   * @param buffer the {@code LongBuffer} instance to dump
   */
  public void dumpBuffer(final LongBuffer buffer) {

    final String bufferClassName = buffer.getClass().getName();
    final String byteBufferFieldName;
    switch (bufferClassName) {
      case "java.nio.ByteBufferAsLongBufferB":
      case "java.nio.ByteBufferAsLongBufferL":
      case "java.nio.ByteBufferAsLongBufferRB":
      case "java.nio.ByteBufferAsLongBufferRL":
        byteBufferFieldName = "bb";
        break;
      case "java.nio.DirectLongBufferU":
      case "java.nio.DirectLongBufferS":
      case "java.nio.DirectLongBufferRU":
      case "java.nio.DirectLongBufferRS":
        byteBufferFieldName = "att";
        break;
      default:
        throw new AssertionError(String.format("LongBuffer type not supported: %s", bufferClassName));
    }

    dumpBuffer(getFieldValue(ByteBuffer.class, buffer, byteBufferFieldName));
  }

  /**
   * Writes the contents of {@link java.nio.ByteBuffer ByteBuffer} to the {@link java.io.PrintStream PrintStream}
   * provided.  The dump consists of a sequence of lines where each line is the dump of a 32-byte segment from
   * the buffer and includes data in hexadecimal and in printable ASCII.  A sample dump is shown: <pre>{@code
   * 00000000  00000000 00000000 00000000 00000023  A5000000 00000000 00000000 00000000  *...............# ................*
   * 00000020  00000000 00000020 00000000 00000023  A5000000 00000000 00000000 00000000  *....... .......# ................*
   * 00000040-00000C7F  duplicates above                                                 *                                 *
   * 00000C80  00000000 00000020 00000000 00000351  00000000 00000000 00000000 00000000  *....... .......Q ................*
   * 00000CA0  00000000 00000000 00000000 00000000  00000000 00000000 00000000 00000000  *................ ................*
   * 00000CC0-00FFFFDF  duplicates above                                                 *                                 *
   * 00FFFFE0  00000000 00000000 00000000 00000000  00000000 00000000 00000000 00000000  *................ ................*
   * }</pre>
   *
   * @param buffer the {@code ByteBuffer} to dump; the buffer is printed from position zero (0) through
   *               the buffer limit using a <i>view</i> over the buffer
   */
  public void dumpBuffer(final ByteBuffer buffer) {

    dumpBufferInfo(buffer);

    final ByteBuffer view = buffer.asReadOnlyBuffer();
    view.clear();

    final int segmentSize = 32;
    final int dumpFormatMax = 8 + 2 + 8 * (segmentSize/4) + (segmentSize/4 - 1) + (segmentSize/16 - 1);
    final int charFormatMax = segmentSize + (segmentSize/16 - 1);

    final StringBuilder dumpBuilder = new StringBuilder(128);
    final Formatter dumpFormatter = new Formatter(dumpBuilder);
    final StringBuilder charBuilder = new StringBuilder(128);

    final byte[][] segments = new byte[2][segmentSize];
    int activeIndex = 0;
    boolean previousSegmentSame = false;
    while (view.hasRemaining()) {

      if (!previousSegmentSame) {
        flushDumpLine(printStream, dumpBuilder, charBuilder);
      }

      int offset = view.position();

      final byte[] activeSegment = segments[activeIndex];
      int segmentLength = Math.min(activeSegment.length, view.remaining());
      view.get(activeSegment, 0, segmentLength);

      /*
       * Except the first segment, perform duplicate segment handling. Duplicate segment data
       * is not dumped; the offset of the first duplicate is retained and printed along with
       * a terminating offset either once a non-duplicate segment or the end of the buffer is
       * reached.
       */
      if (offset != 0) {
        if (view.remaining() != 0 && Arrays.equals(activeSegment, segments[1 - activeIndex])) {
          /* Suppress printing of a segment equal to the previous segment. */
          if (!previousSegmentSame) {
            dumpFormatter.format("%08X", offset);
            previousSegmentSame = true;
          }
          continue;   /* Emit nothing for the 2nd through Nth segments having duplicate data. */

        } else if (previousSegmentSame) {
          /* No longer duplicated; complete duplication marker line and flush. */
          dumpFormatter.format("-%08X  duplicates above", offset - 1);
          dumpBuilder.append(new String(new char[dumpFormatMax - dumpBuilder.length()]).replace('\0', ' '));
          charBuilder.append(new String(new char[charFormatMax]).replace('\0', ' '));
          flushDumpLine(printStream, dumpBuilder, charBuilder);
          previousSegmentSame = false;
        }
      }

      dumpFormatter.format("%08X  ", offset);

      /*
       * Format the segment (or final fragment) data.  Include the character form of each
       * byte if, and only if, it is both (7-bit) ASCII and not a control character.
       */
      for (int i = 0; i < segmentLength; i++) {
        if (i != 0) {
          addGroupSpace(i, dumpBuilder, charBuilder);
        }

        final byte b = activeSegment[i];
        dumpFormatter.format("%02X", b & 0xFF);
        charBuilder.append(ASCII_ENCODER.canEncode((char) b) && !Character.isISOControl(b) ? (char) b : '.');
      }

      activeIndex = 1 - activeIndex;
    }

    /*
     * If the last segment was not a full segment, complete formatting the dump line
     * using filler so separators and such are aligned.
     */
    int segmentOffset = view.position() % segmentSize;
    if (segmentOffset != 0) {
      /* Fill the remaining output buffer */
      for (int i = segmentOffset; i < segmentSize; i++) {
        addGroupSpace(i, dumpBuilder, charBuilder);
        dumpBuilder.append("  ");     /* Empty data */
        charBuilder.append(' ');
      }
    }

    flushDumpLine(printStream, dumpBuilder, charBuilder);
  }

  private void dumpBufferInfo(final Buffer buffer) {
    // Why in the hell is order() not part of Buffer?
    ByteOrder order = ByteOrder.nativeOrder();
    if (buffer instanceof ByteBuffer) {
      order = ((ByteBuffer)buffer).order();
    } else if (buffer instanceof LongBuffer) {
      order = ((LongBuffer)buffer).order();
    } else if (buffer instanceof IntBuffer) {
      order = ((IntBuffer)buffer).order();
    } else if (buffer instanceof CharBuffer) {
      order = ((CharBuffer)buffer).order();
    } else if (buffer instanceof FloatBuffer) {
      order = ((FloatBuffer)buffer).order();
    } else if (buffer instanceof DoubleBuffer) {
      order = ((DoubleBuffer)buffer).order();
    }
    if (buffer.isDirect()) {
      final long address = getFieldValue(Long.class, buffer, "address", true);
      printStream.format("    ByteOrder=%s; capacity=0x%X; limit=0x%X; position=0x%X; address=0x%X%n",
          order, buffer.capacity(), buffer.limit(), buffer.position(), address);
    } else {
      printStream.format("    ByteOrder=%s; capacity=0x%X; limit=0x%X; position=0x%X%n",
          order, buffer.capacity(), buffer.limit(), buffer.position());
    }
  }

  /**
   * {@link java.nio.charset.CharsetEncoder CharsetEncoder} used to test bytes for printability.
   */
  private static CharsetEncoder ASCII_ENCODER = Charset.forName("US-ASCII").newEncoder();

  private static void addGroupSpace(final int i, final StringBuilder dumpBuilder, final StringBuilder charBuilder) {
    if (i % 4 == 0) {
      dumpBuilder.append(' ');
    }
    if (i % 16 == 0) {
      dumpBuilder.append(' ');
      charBuilder.append(' ');
    }
  }

  /**
   * Emit a dump line and prepare the buffers for the next.
   *
   * @param out the {@code PrintStream} to which the dump line is written
   * @param dumpBuilder the {@code StringBuilder} into which the hex portion of the dump line, including
   *                    the displacement, are recorded
   * @param charBuilder the {@code StringBuilder} into which the ASCII portion of the dump line is recorded
   */
  private static void flushDumpLine(final PrintStream out, final StringBuilder dumpBuilder, final StringBuilder charBuilder) {
    if (dumpBuilder.length() != 0) {
      out.append(dumpBuilder).append("  ").append('*').append(charBuilder).append('*');
      out.println();
      dumpBuilder.setLength(0);
      charBuilder.setLength(0);
    }
  }

  /**
   * Gets the value of the designated field from the object instance provided.  This method expects
   * a non-{@code null} value in the field.
   *
   * @param expectedType the type to which the fetched value is cast
   * @param instance the instance from which the value is fetched
   * @param fieldName the name of the field in {@code instance} holding the value
   * @param <V> the declared type of the returned value
   * @param <T> the declared type of the object from which the value is obtained
   *
   * @return the value of the named field
   *
   * @throws AssertionError if the field {@code fieldName} contains {@code null} or
   *      an error occurs while attempting to access the field
   */
  private <V, T> V getFieldValue(final Class<V> expectedType, final T instance, final String fieldName) {
    return getFieldValue(expectedType, instance, fieldName, false);
  }

  /**
   * Gets the value of the designated field from the object instance provided.  This method expects
   * a non-{@code null} value in the field.
   *
   * @param <V> the declared type of the returned value
   * @param <T> the declared type of the object from which the value is obtained
   *
   * @param expectedType the type to which the fetched value is cast
   * @param instance the instance from which the value is fetched
   * @param fieldName the name of the field in {@code instance} holding the value
   * @param quiet if {@code true}, suppress the informational message related to the value fetch and permit
   *              the return of a {@code null} value
   *
   * @return the value of the named field; may be {@code null} if, and only if, {@code quiet} is {@code true}
   *
   * @throws java.lang.AssertionError if the field {@code fieldName} contains {@code null} or
   *      an error occurs while attempting to access the field
   */
  private <V, T> V getFieldValue(final Class<V> expectedType, final T instance, final String fieldName, final boolean quiet) {
    final V fieldValue;
    try {
      /*
       * Traverse the current and super class definitions looking for the named field.
       */
      Class<?> fieldHoldingClass = instance.getClass();
      NoSuchFieldException firstFault = null;
      Field fieldDef = null;
      do {
        try {
          fieldDef = fieldHoldingClass.getDeclaredField(fieldName);
        } catch (NoSuchFieldException e) {
          if (firstFault == null) {
            firstFault = e;
          }
          fieldHoldingClass = fieldHoldingClass.getSuperclass();
          if (fieldHoldingClass == null) {
            throw firstFault;
          }
        }
      } while (fieldDef == null);
      fieldDef.setAccessible(true);
      fieldValue = expectedType.cast(fieldDef.get(instance));
      if (!quiet) {
        if (fieldValue == null) {
          throw new AssertionError(String.format("%s.%s is null; expecting %s instance",
              instance.getClass().getSimpleName(), fieldName, expectedType.getSimpleName()));
        }
        printStream.format("    %s.%s -> %s%n",
            instance.getClass().getSimpleName(), fieldName, getObjectId(fieldValue));
      }

    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new AssertionError(String.format("Unable to get '%s' field from %s instance: %s",
          fieldName, instance.getClass().getName(), e), e);
    }
    return fieldValue;
  }

  /**
   * Gets an object identifier similar to the one produced by {@link Object#toString() Object.toString}.
   *
   * @param o the object for which the identifier is generated
   *
   * @return the object identifier
   */
  public static String getObjectId(final Object o) {
    if (o == null) {
      return "null@0";
    }
    return o.getClass().getName() + "@" + Integer.toHexString(System.identityHashCode(o));
  }
}
