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

import com.terracotta.perf.data.EducationLevel;
import com.terracotta.perf.data.EyeColor;
import com.terracotta.perf.data.Gender;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Date;

@SuppressFBWarnings("EI_EXPOSE_REP")
public class Person implements Serializable {
  private static final long serialVersionUID = 1L;

  protected long id;

  protected String givenName;
  protected String surname;
  protected Gender gender;
  protected Date dateOfBirth;
  protected int age;

  protected float height;
  protected double weight;
  protected EyeColor eyeColor;

  protected Address address;

  /**
   * An indication of the level of education.  Values under 12 indicate the
   * number of years of primary and secondary education (elementary, middle,
   * and high school).  Values 12 and over are keyed by constants in
   * {@link EducationLevel}.
   */
  protected int educationLevel;
  protected String highSchoolName;
  protected String collegeName;

  protected boolean isEnrolled;
  protected Date dateOfJoining;

  protected byte[] rawData;

  public Person() { }

  public Person(long id, String surname, String givenName,
                Gender gender, Date dateOfBirth, int age,
                float height, double weight, final EyeColor eyeColor,
                String street, String city, String state,
                final int educationLevel, final String highSchoolName, final String collegeName,
                boolean isEnrolled, Date dateOfJoining,
                byte[] raw) {
    this.id = id;
    this.surname = surname;
    this.givenName = givenName;
    this.age = age;
    this.dateOfBirth = (dateOfBirth == null ? null : (Date)dateOfBirth.clone());
    this.height = height;
    this.weight = weight;
    this.gender = gender;
    this.eyeColor = eyeColor;

    if (street == null && city == null && state == null) {
      this.address = null;
    } else {
      this.address = new Address(street, city, state);
    }

    this.educationLevel = educationLevel;
    this.highSchoolName = highSchoolName;
    this.collegeName = collegeName;

    this.isEnrolled = isEnrolled;
    this.dateOfJoining = (dateOfJoining == null ? null : (Date)dateOfJoining.clone());
    this.rawData = raw;
  }

  public int getAge() {
    return age;
  }

  public String getName() {
    return givenName + " " + surname;
  }

  public String getSurname() {
    return surname;
  }

  public String getGivenName() {
    return givenName;
  }

  public Gender getGender() {
    return gender;
  }

  public byte[] getRawData() {
    return rawData;
  }

  public Date getDateOfBirth() {
    return dateOfBirth == null ? null : (Date)dateOfBirth.clone();
  }

  public Date getDateOfJoining() {
    return dateOfJoining == null ? null : (Date)dateOfJoining.clone();
  }

  public float getHeight() {
    return height;
  }

  public double getWeight() {
    return weight;
  }

  public long getId() {
    return id;
  }

  public boolean getIsEnrolled() {
    return isEnrolled;
  }

  public Address getAddress() {
    return address;
  }

  public EyeColor getEyeColor() {
    return eyeColor;
  }

  public int getEducationLevel() {
    return educationLevel;
  }

  public String getHighSchoolName() {
    return highSchoolName;
  }

  public String getCollegeName() {
    return collegeName;
  }

  @Override
  public String toString() {
    return  this.getClass().getSimpleName() + "{" +
        "id=" + id +
        ", givenName='" + givenName + '\'' +
        ", surname='" + surname + '\'' +
        ", gender=" + (gender == null ? "null" : gender.name()) +
        ", dateOfBirth=" + dateOfBirth +
        ", age=" + age +
        ", height=" + height +
        ", weight=" + weight +
        ", eyeColor=" + (eyeColor == null ? "null" : eyeColor.name()) +
        ", address=" + address +
        ", educationLevel=" + educationLevel +
        ", highSchoolName='" + highSchoolName + '\'' +
        ", collegeName='" + collegeName + '\'' +
        ", isEnrolled=" + isEnrolled +
        ", dateOfJoining=" + dateOfJoining +
        '}';
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final Person person = (Person)o;

    if (id != person.id) {
      return false;
    }
    if (age != person.age) {
      return false;
    }
    if (Float.compare(person.height, height) != 0) {
      return false;
    }
    if (Double.compare(person.weight, weight) != 0) {
      return false;
    }
    if (educationLevel != person.educationLevel) {
      return false;
    }
    if (isEnrolled != person.isEnrolled) {
      return false;
    }
    if (givenName != null ? !givenName.equals(person.givenName) : person.givenName != null) {
      return false;
    }
    if (!surname.equals(person.surname)) {
      return false;
    }
    if (gender != person.gender) {
      return false;
    }
    if (dateOfBirth != null ? !dateOfBirth.equals(person.dateOfBirth) : person.dateOfBirth != null) {
      return false;
    }
    if (eyeColor != person.eyeColor) {
      return false;
    }
    if (address != null ? !address.equals(person.address) : person.address != null) {
      return false;
    }
    if (highSchoolName != null ? !highSchoolName.equals(person.highSchoolName) : person.highSchoolName != null) {
      return false;
    }
    if (collegeName != null ? !collegeName.equals(person.collegeName) : person.collegeName != null) {
      return false;
    }
    if (dateOfJoining != null ? !dateOfJoining.equals(person.dateOfJoining) : person.dateOfJoining != null) {
      return false;
    }
    return Arrays.equals(rawData, person.rawData);
  }

  @Override
  public int hashCode() {
    int result;
    long temp;
    result = (int)(id ^ (id >>> 32));
    result = 31 * result + (givenName != null ? givenName.hashCode() : 0);
    result = 31 * result + surname.hashCode();
    result = 31 * result + (gender != null ? gender.hashCode() : 0);
    result = 31 * result + (dateOfBirth != null ? dateOfBirth.hashCode() : 0);
    result = 31 * result + age;
    result = 31 * result + (height != +0.0f ? Float.floatToIntBits(height) : 0);
    temp = Double.doubleToLongBits(weight);
    result = 31 * result + (int)(temp ^ (temp >>> 32));
    result = 31 * result + (eyeColor != null ? eyeColor.hashCode() : 0);
    result = 31 * result + (address != null ? address.hashCode() : 0);
    result = 31 * result + educationLevel;
    result = 31 * result + (highSchoolName != null ? highSchoolName.hashCode() : 0);
    result = 31 * result + (collegeName != null ? collegeName.hashCode() : 0);
    result = 31 * result + (isEnrolled ? 1 : 0);
    result = 31 * result + (dateOfJoining != null ? dateOfJoining.hashCode() : 0);
    result = 31 * result + Arrays.hashCode(rawData);
    return result;
  }
}
