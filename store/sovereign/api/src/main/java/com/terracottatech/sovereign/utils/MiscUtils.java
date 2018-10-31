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
package com.terracottatech.sovereign.utils;

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

}
