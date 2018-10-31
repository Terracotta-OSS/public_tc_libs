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
package com.terracottatech.sovereign.common.valuepile;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Write packed integer values. Negative values are handled badly.
 * Values will take 1,2,4 or 5 bytes to encode.
 *
 * @author cschanck
 **/
public class EncodedInteger {
  /*

   encoded length:
   < 64:
      | 0 0 < 6 bit value > |
   < Short.MAX_VALUE>>1 :
      | 01 < 6 bits upper short > < 8 lsbits > |
   < Integer.MAX_VALUE>>1
      | 10 < 6 bits upper int > < 24 lsbits > |
   else
      | 11 < 6 bits 0 > < 32 lsbits > |

   */

  // these use the top 2 bits to encode the type of packing.
  private final static int PACKED_6BITS = 0x00;
  private final static int PACKED_SHORT_ID = 0x40;
  private final static int PACKED_INT_ID = 0x80;
  private final static int FULL_INT_ID = 0xc0;

  public static int write(int length, OutputStream os) throws IOException {
    if (length > 0) {
      if (length < 64) {
        int hdr = PACKED_6BITS | length;
        os.write(hdr);
        return 1;
      } else if (length < (Short.MAX_VALUE >> 1)) {
        os.write(PACKED_SHORT_ID | (length >> 8));
        os.write(length & 0xff);
        return 2;
      } else if (length < Integer.MAX_VALUE >> 1) {
        os.write(PACKED_INT_ID | (length >>> 24));
        os.write((length >>> 16) & 0xff);
        os.write((length >>> 8) & 0xff);
        os.write(length & 0xff);
        return 4;
      }
    }
    // fail on negative.
    // this could be optimized, but length will be positive
    // so ignore it
    os.write(FULL_INT_ID);
    os.write((length >>> 24) & 0xff);
    os.write((length >>> 16) & 0xff);
    os.write((length >>> 8) & 0xff);
    os.write(length & 0xff);
    return 5;
  }

  public static int read(InputStream is) throws IOException {
    int firstByte = is.read();
    switch (firstByte & FULL_INT_ID) {
      case PACKED_6BITS: {
        int ret = firstByte & 0x3f;
        return ret;
      }
      case PACKED_SHORT_ID: {
        int ret = firstByte & 0x3f;
        ret = (ret << 8) | is.read();
        return ret;
      }
      case PACKED_INT_ID: {
        int ret = firstByte & 0x3f;
        ret = (ret << 8) | is.read();
        ret = (ret << 8) | is.read();
        ret = (ret << 8) | is.read();
        return ret;
      }
      case FULL_INT_ID: {
        int ret = is.read();
        ret = (ret << 8) | is.read();
        ret = (ret << 8) | is.read();
        ret = (ret << 8) | is.read();
        return ret;
      }
      default:
        throw new IllegalStateException();
    }
  }

  public static int read(ByteBuffer b) {
    int firstByte = Byte.toUnsignedInt(b.get());
    switch (firstByte & FULL_INT_ID) {
      case PACKED_6BITS: {
        int ret = firstByte & 0x3f;
        return ret;
      }
      case PACKED_SHORT_ID: {
        int ret = firstByte & 0x3f;
        ret = (ret << 8) | Byte.toUnsignedInt(b.get());
        return ret;
      }
      case PACKED_INT_ID: {
        int ret = firstByte & 0x3f;
        ret = (ret << 8) | Byte.toUnsignedInt(b.get());
        ret = (ret << 8) | Byte.toUnsignedInt(b.get());
        ret = (ret << 8) | Byte.toUnsignedInt(b.get());
        return ret;
      }
      case FULL_INT_ID: {
        int ret = Byte.toUnsignedInt(b.get());
        ret = (ret << 8) | Byte.toUnsignedInt(b.get());
        ret = (ret << 8) | Byte.toUnsignedInt(b.get());
        ret = (ret << 8) | Byte.toUnsignedInt(b.get());
        return ret;
      }
      default:
        throw new IllegalStateException();
    }
  }

}
