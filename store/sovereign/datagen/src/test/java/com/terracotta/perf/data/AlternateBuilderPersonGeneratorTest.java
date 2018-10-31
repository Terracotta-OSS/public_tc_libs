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

import org.junit.Test;

import java.util.Random;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.*;

/**
 * Tests construction of a reduced {@code Person} instance.
 *
 * @author Clifford W. Johnson
 */
public class AlternateBuilderPersonGeneratorTest {

  @Test
  public void testGenerator() throws Exception {
    final PersonGenerator<SimplePerson> generator =
        new PersonGenerator<SimplePerson>(new AlternateBuilder(true), 1024)
            .setGenderPercentage(0.0F)
            .setBirthDatePercentage(0.0F)
            .setEducationPercentage(0.0F)
            .setHeightPercentage(0.0F)
            .setEyeColorPercentage(0.0F)
            .setLocationPercentage(0.0F)
            .setTenureDatePercentage(0.0F)
            .setRawDataPercentage(0.0F);

    final Random rnd = new Random(1L);
    for (int i = 0; i < 5; i++) {
      final SimplePerson simplePerson = generator.generate(rnd, i);
      assertThat(simplePerson, is(not(nullValue())));
      assertThat(simplePerson.id, is((long)i));
      assertThat(simplePerson.surname, is(not(nullValue())));
    }
  }

  private static final class AlternateBuilder extends PersonGenerator.PersonBuilder<SimplePerson> {

    protected AlternateBuilder(final boolean resetFields) {
      super(resetFields);
    }

    @Override
    protected SimplePerson newInstance() {
      assertThat(this.getGender(), is(nullValue()));
      assertThat(this.getGivenName(), is(nullValue()));
      assertThat(this.getAge(), is(equalTo(0)));
      assertThat(this.getDateOfBirth(), is(nullValue()));
      assertThat(this.getEyeColor(), is(nullValue()));
      assertThat(this.getHeight(), is(Float.NaN));
      assertThat(this.getWeight(), is(Double.NaN));
      assertThat(this.getStreet(), is(nullValue()));
      assertThat(this.getCity(), is(nullValue()));
      assertThat(this.getState(), is(nullValue()));
      assertThat(this.getEducationLevel(), is(equalTo(EducationLevel.UNKNOWN)));
      assertThat(this.getHighSchoolName(), is(nullValue()));
      assertThat(this.getCollegeName(), is(nullValue()));
      assertThat(this.getDateOfJoining(), is(nullValue()));
      assertThat(this.getRawData(), is(nullValue()));

      return new SimplePerson(
          this.getId(),
          this.getSurname()
      );
    }
  }

  private static final class SimplePerson {
    private final long id;
    private final String surname;

    public SimplePerson(final long id, final String surname) {
      this.id = id;
      this.surname = surname;
    }
  }
}
