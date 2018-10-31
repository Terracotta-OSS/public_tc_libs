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

import java.nio.ByteBuffer;

/**
 * Created by cschanck on 10/14/2016.
 */
public interface RandomValuePileReader {
  int getFieldCount();

  ByteBuffer bytes(int fidx);

  boolean oneBoolean(int fidx);

  byte oneByte(int fidx);

  short oneShort(int fidx);

  char oneChar(int fidx);

  int oneInt(int fidx);

  int encodedInt(int fidx);

  long oneLong(int fidx);

  float oneFloat(int fidx);

  double oneDouble(int fidx);

  String utfString(int fidx);

  ValuePileReader dup(boolean dupBuffer);
}
