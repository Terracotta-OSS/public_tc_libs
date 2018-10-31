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
package com.terracotta.perf.data.sources;

/**
 * Describes US state identification information.
 *
 * @author Clifford W. Johnson
 */
@SuppressWarnings("unused")
public final class UsState {
  private final String name;
  private final short ansiCode;
  private final String postalCode;

  UsState(final String name, final short ansiCode, final String postalCode) {
    this.name = name;
    this.ansiCode = ansiCode;
    this.postalCode = postalCode;
  }

  /**
   * Gets the mixed-case state name.  This is <b>not</b> the full legal
   * name of the state, e.g. "Rhode Island" instead of "State of Rhode Island and Providence Plantations".
   */
  public final String getName() {
    return name;
  }

  /**
   * Gets the ANSI code assigned for the state.
   */
  public final short getAnsiCode() {
    return ansiCode;
  }

  /**
   * Gets the US Postal Service abbreviation for the state.
   */
  public final String getPostalCode() {
    return postalCode;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final UsState usState = (UsState)o;
    return postalCode.equals(usState.postalCode);
  }

  @Override
  public int hashCode() {
    return postalCode.hashCode();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("UsState{");
    sb.append("name='").append(name).append('\'');
    sb.append(", ansiCode=").append(ansiCode);
    sb.append(", postalCode='").append(postalCode).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
