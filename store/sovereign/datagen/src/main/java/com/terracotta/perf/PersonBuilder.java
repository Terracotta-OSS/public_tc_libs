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
package com.terracotta.perf;

import com.terracotta.perf.data.PersonGenerator;

/**
 * A {@link com.terracotta.perf.data.PersonGenerator.PersonBuilder builder} for
 * {@link Person} instances.
 *
 * @author Clifford W. Johnson
 *
 * @see PersonGenerator
 */
public class PersonBuilder extends PersonGenerator.PersonBuilder<Person> {

  /**
   * Constructs a new {@code PersonBuilder}.
   *
   * @param resetFields if {@code true}, the field values will be cleared between
   *                    {@code Person} instances
   */
  public PersonBuilder(final boolean resetFields) {
    super(resetFields);
  }

  @Override
  public Person newInstance() {
    return new Person(
        this.getId(),
        this.getSurname(),
        this.getGivenName(),
        this.getGender(),
        this.getDateOfBirth(),
        this.getAge(),
        this.getHeight(),
        this.getWeight(),
        this.getEyeColor(),
        this.getStreet(),
        this.getCity(),
        this.getState(),
        this.getEducationLevel(),
        this.getHighSchoolName(),
        this.getCollegeName(),
        this.isEnrolled(),
        this.getDateOfJoining(),
        this.getRawData()
    );
  }
}
