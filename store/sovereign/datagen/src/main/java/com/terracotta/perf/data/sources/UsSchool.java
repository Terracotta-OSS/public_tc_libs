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
 * Represents a US primary or secondary school.
 *
 * @author Clifford W. Johnson
 */
public class UsSchool {
  private final String name;
  private final UsState state;
  private final String city;
  private final int enrollment;

  UsSchool(final String name, final UsState state, final String city, final int enrollment) {
    if (name == null) {
      throw new NullPointerException("name");
    }
    if (state == null) {
      throw new NullPointerException("state");
    }
    this.name = name;
    this.state = state;
    this.city = city;
    this.enrollment = enrollment;
  }

  /**
   * Gets the school name.
   */
  public final String getName() {
    return name;
  }

  /**
   * Gets the {@link UsState} representing the state in which the school is located.
   */
  public final UsState getState() {
    return state;
  }

  /**
   * Gets the city/town in which the school is located.
   */
  public final String getCity() {
    return city;
  }

  /**
   * Gets the enrollment of the school.  This value may be approximated.
   */
  public final int getEnrollment() {
    return enrollment;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("UsSchool{");
    sb.append("name='").append(name).append('\'');
    sb.append(", state=").append(state.getPostalCode());
    sb.append(", city='").append(city).append('\'');
    sb.append(", enrollment=").append(enrollment);
    sb.append('}');
    return sb.toString();
  }
}
