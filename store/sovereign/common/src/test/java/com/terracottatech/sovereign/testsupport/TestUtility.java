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

/**
 * A mish-mash of utility methods supporting testing.
 *
 * @author Clifford W. Johnson
 */
public class TestUtility {
  private TestUtility() {
  }

  /**
   * Formats a nanosecond time into a {@code String} of the form
   * {@code <hours> h <minutes> m <seconds>.<fractional_seconds> s}.
   *
   * @param millis tbe millisecond value to convert
   *
   * @return the formatted result
   */
  public static CharSequence formatMillis(final long millis) {
    final int elapsedMillis = (int)(millis % 1_000);
    final long elapsedTimeSeconds = millis / 1_000;
    final int elapsedSeconds = (int)(elapsedTimeSeconds % 60);
    final long elapsedTimeMinutes = elapsedTimeSeconds / 60;
    final int elapsedMinutes = (int)(elapsedTimeMinutes % 60);
    final long elapsedHours = elapsedTimeMinutes / 60;

    final StringBuilder sb = new StringBuilder(128);
    if (elapsedHours > 0) {
      sb.append(elapsedHours).append(" h ");
    }
    if (elapsedHours > 0 || elapsedMinutes > 0) {
      sb.append(elapsedMinutes).append(" m ");
    }
    sb.append(elapsedSeconds).append('.').append(String.format("%03d", elapsedMillis)).append(" s");

    return sb;
  }

  /**
   * Formats a nanosecond time into a {@code String} of the form
   * {@code <hours> h <minutes> m <seconds>.<fractional_seconds> s}.
   *
   * @param nanos tbe nanosecond value to convert
   *
   * @return the formatted result
   */
  public static CharSequence formatNanos(final long nanos) {
    final int elapsedNanos = (int)(nanos % 1_000_000_000);
    final long elapsedTimeSeconds = nanos / 1_000_000_000;
    final int elapsedSeconds = (int)(elapsedTimeSeconds % 60);
    final long elapsedTimeMinutes = elapsedTimeSeconds / 60;
    final int elapsedMinutes = (int)(elapsedTimeMinutes % 60);
    final long elapsedHours = elapsedTimeMinutes / 60;

    final StringBuilder sb = new StringBuilder(128);
    if (elapsedHours > 0) {
      sb.append(elapsedHours).append(" h ");
    }
    if (elapsedHours > 0 || elapsedMinutes > 0) {
      sb.append(elapsedMinutes).append(" m ");
    }
    sb.append(elapsedSeconds).append('.').append(String.format("%09d", elapsedNanos)).append(" s");

    return sb;
  }
}
