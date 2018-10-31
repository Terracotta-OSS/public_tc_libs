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
package com.terracotta.perf.data;

/**
 * Constants representing levels of formal education.
 * Values under 12 indicate the number of years of primary and secondary education
 * (elementary, middle, and high school).  Values 12 and over indicate the degree or
 * certificated earned.
 *
 * @author Clifford W. Johnson
 */
public final class EducationLevel {

  /**
   * Indicates an education level is not available.
   */
  public static final int UNKNOWN = -1;
  public static final int NONE = 0;
  /**
   * High school diploma or GED.
   */
  public static final int HIGH_SCHOOL_GRAD = 12;
  /**
   * Education after high school short of or not leading to a college degree.
   */
  public static final int POST_SECONDARY = 13;
  public static final int ASSOCIATE_DEGREE = 14;
  public static final int BACHELOR_DEGREE = 16;
  /**
   * Education after earning a bachelor's degree but short of a master's degree.
   */
  public static final int POST_GRADUATE = 17;
  public static final int MASTER_DEGREE = 18;
  public static final int DOCTORAL_DEGREE = 20;
  /**
   * Education after a doctoral degree.
   */
  public static final int POST_DOCTORAL = 22;

  public static final int MIN_VALUE = NONE;
  public static final int MAX_VALUE = POST_DOCTORAL;
}
