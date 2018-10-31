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

import com.terracotta.perf.data.EducationLevel;

/**
 * Describes a US post-secondary school.  This includes colleges, universities, and trade schools.
 *
 * @author Clifford W. Johnson
 */
public final class UsPostSecondarySchool extends UsSchool {

  /**
   * Indicates the highed degree level offered.  A value from
   * {@link EducationLevel EducationLevel} is
   * used.
   */
  private final int highestDegreeOffered;

  /**
   * Indicates if this school offers Associate's or Bachelor's degrees.
   */
  private final boolean offersUndergraduateDegree;

  UsPostSecondarySchool(final String name, final UsState state, final String city, final int enrollment, final int highestDegreeOffered, final boolean offersUndergraduateDegree) {
    super(name, state, city, enrollment);
    this.highestDegreeOffered = highestDegreeOffered;
    this.offersUndergraduateDegree = offersUndergraduateDegree;
  }

  /**
   * Gets the {@link EducationLevel EducationLevel} representing the
   * highest degree or certificate offered.
   */
  public int getHighestDegreeOffered() {
    return highestDegreeOffered;
  }

  /**
   * Indicates whether or not this school offers a Bachelor's degree or below.
   */
  public boolean offersUndergraduateDegree() {
    return offersUndergraduateDegree;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("UsPostSecondarySchool{");
    sb.append("name='").append(this.getName()).append('\'');
    sb.append(", state=").append(this.getState().getPostalCode());
    sb.append(", city='").append(this.getCity()).append('\'');
    sb.append(", enrollment=").append(this.getEnrollment());
    sb.append(", highestDegreeOffered=").append(highestDegreeOffered);
    sb.append(", offersUndergraduateDegree=").append(offersUndergraduateDegree);
    sb.append('}');
    return sb.toString();
  }
}
