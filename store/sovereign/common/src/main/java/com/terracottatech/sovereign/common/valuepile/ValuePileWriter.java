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
import java.io.OutputStream;

/**
 * @author cschanck
 **/
public interface ValuePileWriter {

  void resetCurrent() throws IOException;

  void reset(OutputStream dos) throws IOException;

  ValuePileWriter bytes(byte[] b, int off, int len) throws IOException;

  ValuePileWriter oneBoolean(boolean v) throws IOException;

  ValuePileWriter oneByte(int v) throws IOException;

  ValuePileWriter oneShort(int v) throws IOException;

  ValuePileWriter oneChar(int v) throws IOException;

  ValuePileWriter oneInt(int v) throws IOException;

  ValuePileWriter encodedInt(int v) throws IOException;

  ValuePileWriter oneLong(long v) throws IOException;

  ValuePileWriter oneFloat(float v) throws IOException;

  ValuePileWriter oneDouble(double v) throws IOException;

  ValuePileWriter utfString(String s) throws IOException;

  int finish() throws IOException;

  int getFieldCount();

  static ValuePileWriter writer(OutputStream os, int skipBytes) throws IOException {
    return new ValuePileWriterImpl(os, skipBytes);
  }

  static ValuePileWriter writer(int skipBytes) {
    return new ValuePileWriterImpl(skipBytes);
  }
}
