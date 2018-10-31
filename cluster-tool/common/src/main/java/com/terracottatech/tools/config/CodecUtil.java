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
package com.terracottatech.tools.config;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;


public class CodecUtil {

  public static byte[] streamToBytes(InputStream inputStream) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      int read;
      while((read = inputStream.read()) != -1) {
        baos.write(read);
      }
    } catch (IOException e) {
      return null;
    }
    return baos.toByteArray();
  }
}
