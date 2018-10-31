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
package com.terracottatech.sovereign.common.dumbstruct.buffers;

import java.nio.ByteBuffer;

/**
 * @author cschanck
 */
public interface DataBuffer {

  int size();

  byte get(int index);

  DataBuffer put(int index, byte b);

  char getChar(int index);

  DataBuffer putChar(int index, char c);

  short getShort(int index);

  DataBuffer putShort(int index, short value);

  int getInt(int index);

  DataBuffer putInt(int index, int value);

  float getFloat(int index);

  DataBuffer putFloat(int index, float value);

  long getLong(int index);

  DataBuffer putLong(int index, long value);

  double getDouble(int index);

  DataBuffer putDouble(int index, double value);

  DataBuffer getString(int index, char[] dest, int start, int length);

  DataBuffer putString(CharSequence src, int start, int end, int index);

  DataBuffer copyWithin(int fromIndex, int toIndex, int many);

  DataBuffer put(byte[] barr, int offset, int many, int toIndex);

  DataBuffer get(int fromIndex, byte[] barr, int offset, int many);

  DataBuffer put(ByteBuffer b, int toIndex);

  DataBuffer get(int fromIndex, ByteBuffer b);

  void fill(byte b);

  void fill(int offset, int size, byte value);

  DataBuffer put(int destIndex, DataBuffer buffer, int srcIndex, int many);

}
