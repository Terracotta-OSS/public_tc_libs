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

import com.terracotta.perf.data.sources.AgeDistribution;
import com.terracotta.perf.data.sources.BodyData;
import com.terracotta.perf.data.sources.EyeColorData;
import com.terracotta.perf.data.sources.GoogleGeolocationData;
import com.terracotta.perf.data.sources.GoogleGeolocationData.UsCity;
import com.terracotta.perf.data.sources.UsCensusData;
import com.terracotta.perf.data.sources.UsK12SchoolData;
import com.terracotta.perf.data.sources.UsPostSecondaryData;

import java.util.Calendar;
import java.util.Date;
import java.util.Random;
import java.util.TimeZone;

/**
 * Generates instances of a generic person object ({@code P}) using pseudo-random content.
 * <p>
 * This generator relies on data values obtained from various public sources
 * in an attempt to provide {@code Person} values containing something
 * approximating real-world data.
 * <p>
 * By default, this generator produces, through a {@link PersonBuilder PersonBuilder}, a
 * person instance that
 * includes a full complement of builder field values.  Whether or not
 * a specific field has a value is based on a weighted distribution for that
 * field.  For example, while a surname will always be generated for a person instance,
 * by default only 90% will contain a given name.  The percentage of person
 * instances to contain a given field value may be controlled through the
 * <code><i>set...Percentage</i></code> methods.  Due to the expense in their production,
 * values for some fields may be omitted by configuration -- see the
 * <code><i>include</i>...</code> methods.
 * <p>
 * Values for fields are generated using data from several sources, some internal
 * and some external.  Value selection may be uniformly random, a Gaussian distribution,
 * or distributed according to externally-obtained population weightings.
 * <p>
 * To use this generator under Rainfall, create a subclass of this class with a
 * {@code generate(long)} method implementation of the following form:
 * <pre><code>
 * import io.rainfall.ObjectGenerator;
 * import com.terracotta.perf.Person;
 * import com.terracotta.perf.data.PersonGenerator;
 * import java.util.Random;
 *
 * public class RainfallPersonGenerator extends PersonGenerator&lt;Person&gt; implements ObjectGenerator&lt;Person&gt; {
 *   public static final long ID_FACTOR = 500000000;
 *
 *   public RainfallPersonGenerator(final int size) {
 *     super(new com.terracotta.perf.PersonBuilder(true), size);
 *     this.setBirthDatePercentage(100.0F);    // Change from default of 90.0%
 *     this.setGenderPercentage(100.0F);       // Change from default of 95.0%
 *   }
 *
 *   &#64;Override
 *   public Person generate(Long seed) {
 *     return generate(new Random(seed), ID_FACTOR + seed);
 *   }
 *
 *   &#64;Override
 *   public String getDescription() {
 *     return "PersonGenerator";
 *   }
 * } * </code></pre>
 *
 * @param <P> type of {@code Person} instances to generate
 */
// TODO: Add title selection
public class PersonGenerator<P> {

  /**
   * The date used to provide a stable "current" time from which test
   * date/time values can be generated.  No dates generated as test data in
   * this class should fall chronologically after this date.
   */
  private static final Calendar DEFAULT_BASE_DATE;
  static {
    final Calendar baseDate = Calendar.getInstance(TimeZone.getTimeZone("US/Pacific"));
    baseDate.clear();
    baseDate.set(2015, Calendar.DECEMBER, 14);    // Star Wars: The Force Awakens premiere
    DEFAULT_BASE_DATE = baseDate;
  }

  private static final float DEFAULT_GENDER_PERCENTAGE = 95.0F;
  private static final float DEFAULT_BIRTH_DATE_PERCENTAGE = 90.0F;
  private static final float DEFAULT_TENURE_DATE_PERCENTAGE = 50.0F;
  private static final float DEFAULT_HEIGHT_PERCENTAGE = 75.0F;
  private static final float DEFAULT_EYE_COLOR_PERCENTAGE = 90.0F;
  private static final float DEFAULT_EDUCATION_PERCENTAGE = 100.0F;
  private static final float DEFAULT_LOCATION_PERCENTAGE = 85.0F;
  private static final float DEFAULT_RAW_DATA_PERCENTAGE = 80.0F;

  /**
   * The maximum number of years a Person can be "joined".
   */
  private static final int MAX_TENURE = 35;

  /**
   * Builder to use for {@code Person} construction.
   */
  private final PersonBuilder<P> builder;

  /**
   * The maximum number of bytes of random data to generate for a {@code byte[]} field.
   */
  private final int maxByteSize;

  /**
   * The date from which age & tenure are calculated.
   */
  private Calendar baseDate = (Calendar)DEFAULT_BASE_DATE.clone();

  /**
   * Percentage of generated {@code Person} instances to receive a gender.
   * This value also controls the generation of given names and affects
   * generation of height and weight values.
   */
  private float genderPercentage = DEFAULT_GENDER_PERCENTAGE;

  /**
   * Percentage of generated {@code Person} instances to receive a birth date and age.
   */
  private float birthDatePercentage = DEFAULT_BIRTH_DATE_PERCENTAGE;

  /**
   * Percentage of generated {@code Person} instances to receive a date of joining.
   */
  private float tenureDatePercentage = DEFAULT_TENURE_DATE_PERCENTAGE;

  /**
   * Percentage of generated {@code Person} instances to receive a height and weight.
   * This percentage is <i>within</i> {@code Person} instances for which a gender
   * is specified.
   */
  private float heightPercentage = DEFAULT_HEIGHT_PERCENTAGE;

  /**
   * Percentage of generated {@code Person} instances to receive an eye color.
   */
  private float eyeColorPercentage = DEFAULT_EYE_COLOR_PERCENTAGE;

  /**
   * Percentage of generated {@code Person} instances to receive education information.
   */
  private float educationPercentage = DEFAULT_EDUCATION_PERCENTAGE;

  /**
   * Percentage of generated {@code Person} instances to receive a location (street, city, state).
   */
  private float locationPercentage = DEFAULT_LOCATION_PERCENTAGE;

  /**
   * Percentage of generated {@code Person} instances to receive a raw data object.
   */
  private float rawDataPercentage = DEFAULT_RAW_DATA_PERCENTAGE;

  /**
   * Indicates if a high school name is included in generated {@code Person} instances.
   */
  private boolean includeHighSchool = true;

  /**
   * Indicates if a post-secondary school name is included in generated {@code Person} instances.
   */
  private boolean includeCollege = true;

  /**
   * Indicates if a given (first) name is included in generated {@code Person} instances.
   */
  private boolean includeGivenName = true;

  /**
   * Construct a {@code PeopleObjectGenerator}.  This generator produces
   * instances of the a generic person object ({@code P}) with
   * pseudo-random content.
   *
   * @param builder the {@code PersonBuilder} instance used for {@code P} creation
   * @param size the maximum number of random bytes to include in as raw data
   */
  public PersonGenerator(final PersonBuilder<P> builder, int size) {
    this.builder = builder;
    this.maxByteSize = size;
  }

  /**
   * Sets the date from which age and tenure are calculated.  During these calculations,
   * a random date from within the year preceding the base date is chosen and the age (or
   * tenure) subtracted from that date to yield the origin date (birth date or date of joining).
   * <p>
   * To ensure meaningful origin dates, the base date should be set to a date not after the
   * current date.  For stable test values, a fixed {@code TimeZone} should be set in the
   * {@code Calendar} instance provided.
   * <p>
   * The default base date is 00:00:00 hours on December 14, 2015 US/Pacific time.
   *
   * @param baseDate the date from which the age-based origin date is calculated
   *
   * @return this {@code PersonGenerator}
   */
  public final PersonGenerator<P> setBaseDate(final Calendar baseDate) {
    this.baseDate = (Calendar)baseDate.clone();
    return this;
  }

  /**
   * Sets the percentage of {@code P} instances for which a gender is included.
   * This also controls given name.  The default value is <code>{@value #DEFAULT_GENDER_PERCENTAGE}%</code>.
   * Setting this value to 0.0F suppresses gender, given name, height, and weight generation.
   *
   * @param genderPercentage a non-negative value between 0.0F and 100.0F, inclusive, indicating the
   *                         percentage of generated {@code P} instances to receive a gender and given name
   *
   * @return this {@code PersonGenerator}
   */
  public final PersonGenerator<P> setGenderPercentage(final float genderPercentage) {
    this.genderPercentage = this.checkPercentage(genderPercentage);
    return this;
  }

  /**
   * Sets the percentage of {@code P} instances for which a birth date is included.
   * This also controls age.  The default value is <code>{@value #DEFAULT_BIRTH_DATE_PERCENTAGE}%</code>.
   * Setting this value to 0.0F suppresses birth date and age generation.
   *
   * @param birthDatePercentage a non-negative value between 0.0F and 100.0F, inclusive, indicating the
   *                         percentage of generated {@code P} instances to receive a birth date and age
   *
   * @return this {@code PersonGenerator}
   */
  public final PersonGenerator<P> setBirthDatePercentage(final float birthDatePercentage) {
    this.birthDatePercentage = this.checkPercentage(birthDatePercentage);
    return this;
  }

  /**
   * Sets the percentage of {@code P} instances for which a date of joining is included.
   * The default value is <code>{@value #DEFAULT_TENURE_DATE_PERCENTAGE}%</code>.
   * Setting this value to 0.0F suppresses date of joining generation.
   *
   * @param tenureDatePercentage a non-negative value between 0.0F and 100.0F, inclusive, indicating the
   *                         percentage of generated {@code P} instances to receive a date of joining
   *
   * @return this {@code PersonGenerator}
   */
  public final PersonGenerator<P> setTenureDatePercentage(final float tenureDatePercentage) {
    this.tenureDatePercentage = this.checkPercentage(tenureDatePercentage);
    return this;
  }

  /**
   * Sets the percentage of {@code P} instances for which a height and weight are included.
   * The default value is <code>{@value #DEFAULT_HEIGHT_PERCENTAGE}%</code>.
   * Setting this value to 0.0F suppresses height and weight generation.
   * <p>
   * Only {@code P} instances receiving a gender are subject to this percentage -- height
   * and weight calculations are gender-specific and are not calculated when a gender is
   * not available.
   *
   * @param heightPercentage a non-negative value between 0.0F and 100.0F, inclusive, indicating the
   *                         percentage of generated {@code P} instances to receive a height and weight
   *
   * @return this {@code PersonGenerator}
   */
  public final PersonGenerator<P> setHeightPercentage(final float heightPercentage) {
    this.heightPercentage = this.checkPercentage(heightPercentage);
    return this;
  }

  /**
   * Sets the percentage of {@code P} instances for which an eye color is included.
   * The default value is <code>{@value #DEFAULT_EYE_COLOR_PERCENTAGE}%</code>.
   * Setting this value to 0.0F suppresses eye color generation.
   *
   * @param eyeColorPercentage a non-negative value between 0.0F and 100.0F, inclusive, indicating the
   *                         percentage of generated {@code P} instances to receive an eye color
   *
   * @return this {@code PersonGenerator}
   */
  public final PersonGenerator<P> setEyeColorPercentage(final float eyeColorPercentage) {
    this.eyeColorPercentage = this.checkPercentage(eyeColorPercentage);
    return this;
  }

  /**
   * Sets the percentage of {@code P} instances for which education information is included.
   * The default value is <code>{@value #DEFAULT_EDUCATION_PERCENTAGE}%</code>.
   * Setting this value to 0.0F suppresses education information generation.
   *
   * @param educationPercentage a non-negative value between 0.0F and 100.0F, inclusive, indicating the
   *                         percentage of generated {@code P} instances to receive education information
   *
   * @return this {@code PersonGenerator}
   */
  public final PersonGenerator<P> setEducationPercentage(final float educationPercentage) {
    this.educationPercentage = this.checkPercentage(educationPercentage);
    return this;
  }

  /**
   * Sets the percentage of {@code P} instances for which a location (street, city, state) is included.
   * The default value is <code>{@value #DEFAULT_LOCATION_PERCENTAGE}%</code>.
   * Setting this value to 0.0F suppresses location generation.
   *
   * @param locationPercentage a non-negative value between 0.0F and 100.0F, inclusive, indicating the
   *                         percentage of generated {@code P} instances to receive a street, city, and
   *                         state
   *
   * @return this {@code PersonGenerator}
   */
  public final PersonGenerator<P> setLocationPercentage(final float locationPercentage) {
    this.locationPercentage = this.checkPercentage(locationPercentage);
    return this;
  }

  /**
   * Sets the percentage of {@code P} instances for which a raw data object is included.
   * The default value is <code>{@value #DEFAULT_RAW_DATA_PERCENTAGE}</code>.
   * Setting this value to 0.0F suppresses raw data generation.
   *
   * @param rawDataPercentage a non-negative value between 0.0F and 100.0F, inclusive, indicating the
   *                          percentage of generated {@code P} instances to receive raw data
   *
   * @return this {@code PersonGenerator}
   */
  public final PersonGenerator<P> setRawDataPercentage(final float rawDataPercentage) {
    this.rawDataPercentage = this.checkPercentage(rawDataPercentage);
    return this;
  }

  /**
   * Sets whether or not a given name is assigned to the generated {@code Person} instances.
   * The initial value is {@code true}.
   *
   * @param setting {@code false} to set the given name to {@code null}; {@code true} to choose
   *                             a given name according to gender
   *
   * @return this {@code PersonGenerator}
   */
  public final PersonGenerator<P> includeGivenName(final boolean setting) {
    this.includeGivenName = setting;
    return this;
  }

  /**
   * Sets whether or not a high school is assigned to the generated {@code Person} instances.
   * The initial value is {@code true}.
   *
   * @param setting {@code false} to set the high school to {@code null}; {@code true} to choose
   *                             a high school based on state and level of education
   *
   * @return this {@code PersonGenerator}
   */
  public final PersonGenerator<P> includeHighSchool(final boolean setting) {
    this.includeHighSchool = setting;
    return this;
  }

  /**
   * Sets whether or not a post-secondary school (college, university, trade school) is assigned
   * to generated {@code Person} instances.
   *
   * @param setting {@code false} to set the post-secondary school to {@code null}; {@code true}
   *                             to choose a post-secondary school based on level of education
   *
   * @return this {@code PersonGenerator}
   */
  public final PersonGenerator<P> includeCollege(final boolean setting) {
    this.includeCollege = setting;
    return this;
  }

  private float checkPercentage(final float value) {
    if (Float.compare(value, 0.0F) < 0 || Float.compare(value, 100.0F) > 0) {
      throw new IllegalArgumentException("Percentage value not between 0.0F and 100.0F - " + value);
    }
    return value;
  }

  /**
   * Generates a new {@code Person} instance using the random number source and id provided.
   *
   * @param rnd the {@code Random} instance to use in field value generation
   * @param id the identifier to assign to the new {@code Person}
   *
   * @return a new {@code Person} instance
   */
  public P generate(final Random rnd, final long id) {
    this.builder.setId(id);

    final Calendar now = Calendar.getInstance();

    /*
     * Determine gender based on census distribution.
     */
    Gender gender;
    if (weightedTrue(rnd, genderPercentage)) {
      gender = UsCensusData.chooseGender(rnd);
    } else {
      gender = null;
    }
    this.builder.setGender(gender);

    /*
     * Always choose a surname; choose a given name if we have a gender.
     */
    this.builder.setSurname(UsCensusData.chooseSurname(rnd));
    String givenName;
    if (gender != null && this.includeGivenName) {
      if (gender == Gender.FEMALE) {
        givenName = UsCensusData.chooseFemaleName(rnd);
      } else {
        givenName = UsCensusData.chooseMaleName(rnd);
      }
    } else {
      givenName = null;
    }
    this.builder.setGivenName(givenName);

    /*
     * Generate a birth date (and age) in the desired range.
     * A birth date is calculated base on a randomly selected age on the date specified
     * in BASE_DATE.  The actual age is calculated as of the time of the test.
     */
    Date dob;
    int age;
    if (weightedTrue(rnd, birthDatePercentage)) {
      final Calendar date = generateDate(rnd, this.getBirthAgeDistribution().getAge(rnd));
      dob = date.getTime();
      // TODO: Should age be left as the age as of BASE_DATE instead of now?
      age = yearDiff(date, now);
    } else {
      dob = null;
      age = 0;
    }
    this.builder.setDateOfBirth(dob);
    this.builder.setAge(age);

    /*
     * Generate a height in the desired range and a weight based on the height.
     */
    float height;
    double weight;
    if (gender != null && weightedTrue(rnd, heightPercentage)) {
      height = generateHeight(rnd, gender);
      weight = generateWeight(rnd, gender, height);
    } else {
      height = Float.NaN;
      weight = Double.NaN;
    }
    this.builder.setHeight(height);
    this.builder.setWeight(weight);

    /*
     * Select an eye color.
     */
    EyeColor eyeColor;
    if (weightedTrue(rnd, eyeColorPercentage)) {
      eyeColor = EyeColorData.chooseEyeColor(rnd);
    } else {
      eyeColor = null;
    }
    this.builder.setEyeColor(eyeColor);

    /*
     * Calculate an address.
     */
    String street;
    String city;
    String state;
    if (weightedTrue(rnd, locationPercentage)) {
      UsCity usCity = GoogleGeolocationData.chooseCity(rnd);
      city = usCity.getName();
      state = usCity.getState();

      if (rnd.nextBoolean()) {
        street = id + " random Street";
      } else {
        street = null;
      }
    } else {
      street = null;
      city = null;
      state = null;
    }
    this.builder.setStreet(street);
    this.builder.setCity(city);
    this.builder.setState(state);

    /*
     * Generate an education level.  If a high school grad and we have a state of residence,
     * choose a high school from that state.  If post-secondary education is indicated,
     * choose a post-secondary school based on the highest degree earned.  The post-secondary
     * school choice is made without regard to state of residence.
     */
    final int educationLevel;
    final String highSchoolName;
    final String collegeName;
    if (weightedTrue(rnd, educationPercentage)) {
      educationLevel = UsCensusData.chooseEducationLevel(rnd);
      if (this.includeHighSchool && educationLevel >= EducationLevel.HIGH_SCHOOL_GRAD && state != null) {
        highSchoolName = UsK12SchoolData.getUsHighSchoolInState(rnd, UsCensusData.getUsStateByName(state)).getName();
      } else {
        highSchoolName = null;
      }

      if (this.includeCollege && educationLevel >= EducationLevel.POST_SECONDARY) {
        collegeName = UsPostSecondaryData.getUsPostSecondarySchoolForEducationLevel(rnd, educationLevel).getName();
      } else {
        collegeName = null;
      }
    } else {
      educationLevel = EducationLevel.UNKNOWN;
      highSchoolName = null;
      collegeName = null;
    }
    this.builder.setEducationLevel(educationLevel);
    this.builder.setHighSchoolName(highSchoolName);
    this.builder.setCollegeName(collegeName);

    this.builder.setEnrolled(rnd.nextBoolean());

    /*
     * Generate a tenure based on the current age, if known.
     */
    Date doj;
    if (weightedTrue(rnd, tenureDatePercentage)) {
      final int maxTenure = (age == 0 ? MAX_TENURE : Math.min(MAX_TENURE, age - this.getBirthAgeDistribution().getLowestAge()));
      final Calendar date = generateDate(rnd, rnd.nextInt(1 + maxTenure));
      doj = date.getTime();
    } else {
      doj = null;
    }
    this.builder.setDateOfJoining(doj);

    /*
     * Generate some raw data.
     */
    byte[] rawBytes;
    if (weightedTrue(rnd, rawDataPercentage)) {
      final int byteSize = rnd.nextInt(this.maxByteSize);
      rawBytes = new byte[byteSize];
      rnd.nextBytes(rawBytes);
    } else {
      rawBytes = null;
    }
    this.builder.setRawData(rawBytes);

    return this.builder.build();
  }

  /**
   * Gets the distribution of ages according to the US Census.
   */
  private AgeDistribution getBirthAgeDistribution() {
    return UsCensusData.getBirthAgeDistribution();
  }

  /**
   * Returns a pseudo-random boolean weighted to provide a specified percentage of {@code true}
   * values.
   *
   * @param rnd the {@code Random} instance to use for the calculation
   * @param weight the weight, expressed as a percent from 0.0 to 100.0), applied to {@code true} value return
   *
   * @return the pseudo-random, weighted boolean
   */
  private boolean weightedTrue(final Random rnd, final float weight) {
    return (100.0F * rnd.nextFloat() <= weight);
  }

  /**
   * Generates a random date, before the {@link #setBaseDate(Calendar) base date}, within the year-bounds specified.
   *
   * @param rnd the {@code Random} instance to use for the calculation
   * @param age the age, in years, for which the date is calculated
   *
   * @return a new Calendar instance
   */
  private Calendar generateDate(final Random rnd, final int age) {
    final Calendar date = (Calendar)this.baseDate.clone();
    date.add(Calendar.DAY_OF_YEAR, -(rnd.nextInt(365)));    // Back date off a portion of a year
    date.add(Calendar.YEAR, -age);
    return date;
  }

  /**
   * Generates a gender-appropriate height.  The height is chosen using
   * {@link BodyData#chooseHeight(Random, Gender) BodyData.chooseHeight}.
   *
   * @param rnd the {@code Random} instance to use for the calculation
   * @param gender the gender used to bias the computation
   *
   * @return a height
   */
  private float generateHeight(final Random rnd, final Gender gender) {
    return BodyData.chooseHeight(rnd, gender);
  }

  /**
   * Calculates a weight appropriate for the gender and height.
   * Generates a weight using a Body Mass Index (BMI) calculation.  The BMI is chosen
   * using {@link BodyData#chooseBMI(Random, Gender) BodyData.chooseBMI}.
   *
   * @param rnd the {@code Random} instance to use for BMI selection
   * @param gender the gender used to bias the computation
   * @param height the height on which the calculation is based
   *
   * @return the weight, in pounds
   */
  private double generateWeight(final Random rnd, final Gender gender, final float height) {
    return BodyData.calculateWeight(rnd, gender, height);
  }

  /**
   * Determines the difference, in whole years (age), between two dates.
   *
   * @param baseDate the base, or earlier, date
   * @param compareDate the later/current date
   *
   * @return the number of whole years between {@code baseDate} and {@code compareDate}
   */
  private int yearDiff(final Calendar baseDate, final Calendar compareDate) {
    int yearDiff = compareDate.get(Calendar.YEAR) - baseDate.get(Calendar.YEAR);
    if (compareDate.get(Calendar.MONTH) < baseDate.get(Calendar.MONTH)
        || (compareDate.get(Calendar.MONTH) == baseDate.get(Calendar.MONTH)
        && compareDate.get(Calendar.DAY_OF_MONTH) < baseDate.get(Calendar.DAY_OF_MONTH))) {
      yearDiff--;
    }
    return yearDiff;
  }

  /**
   * Defines a builder, using field values generated by {@link PersonGenerator}, for generic
   * {@code Person} instances.  This class provides accessors for all fields generated by
   * {@link PersonGenerator}.
   *
   * @param <P> the specific {@code Person} type
   */
  public abstract static class PersonBuilder<P> {

    /**
     * Indicates that the field values are to be reset once a {@code P} is generated.
     */
    private final boolean resetFields;

    /**
     * Person instance identifier.
     */
    private long id;

    private String surname;
    private String givenName;
    private Gender gender;
    private Date dateOfBirth;
    private int age;
    private float height = Float.NaN;
    private double weight = Double.NaN;
    private EyeColor eyeColor;
    private String street;
    private String city;
    private String state;
    private int educationLevel;
    private String highSchoolName;
    private String collegeName;
    private boolean enrolled;
    private Date dateOfJoining;
    private byte[] rawData;

    /**
     * Creates a new {@code PersonBuilder} instance.
     *
     * @param resetFields if {@code true}, the field values are reset after each
     *                    {@code P} instance is built
     */
    protected PersonBuilder(final boolean resetFields) {
      this.resetFields = resetFields;
    }

    /**
     * Creates a new {@code Person} instance.
     *
     * @return a new {@code P}
     */
    protected abstract P newInstance();

    /**
     * Builds a new {@code Person} and optionally resets the field values;
     *
     * @return a new {@code P}
     */
    private P build() {
      final P person = this.newInstance();
      if (this.resetFields) {
        resetFields();
      }
      return person;
    }

    /**
     * Resets the values of each {@code PersonBuilder} field.
     */
    protected void resetFields() {
      this.id = 0L;
      this.surname = null;
      this.givenName = null;
      this.gender = null;
      this.dateOfBirth = null;
      this.age = 0;
      this.height = Float.NaN;
      this.weight = Double.NaN;
      this.eyeColor = null;
      this.street = null;
      this.city = null;
      this.state = null;
      this.educationLevel = EducationLevel.UNKNOWN;
      this.highSchoolName = null;
      this.collegeName = null;
      this.enrolled = false;
      this.dateOfJoining = null;
      this.rawData = null;
    }

    protected long getId() {
      return id;
    }

    private PersonBuilder<P> setId(final long id) {
      this.id = id;
      return this;
    }

    protected String getSurname() {
      return surname;
    }

    private PersonBuilder<P> setSurname(final String surname) {
      this.surname = surname;
      return this;
    }

    protected String getGivenName() {
      return givenName;
    }

    private PersonBuilder<P> setGivenName(final String givenName) {
      this.givenName = givenName;
      return this;
    }

    protected Gender getGender() {
      return gender;
    }

    private PersonBuilder<P> setGender(final Gender gender) {
      this.gender = gender;
      return this;
    }

    protected Date getDateOfBirth() {
      return dateOfBirth;
    }

    private PersonBuilder<P> setDateOfBirth(final Date dateOfBirth) {
      this.dateOfBirth = dateOfBirth;
      return this;
    }

    protected int getAge() {
      return age;
    }

    private PersonBuilder<P> setAge(final int age) {
      this.age = age;
      return this;
    }

    protected float getHeight() {
      return height;
    }

    private PersonBuilder<P> setHeight(final float height) {
      this.height = height;
      return this;
    }

    protected double getWeight() {
      return weight;
    }

    private PersonBuilder<P> setWeight(final double weight) {
      this.weight = weight;
      return this;
    }

    protected EyeColor getEyeColor() {
      return eyeColor;
    }

    private PersonBuilder<P> setEyeColor(final EyeColor eyeColor) {
      this.eyeColor = eyeColor;
      return this;
    }

    protected String getStreet() {
      return street;
    }

    private PersonBuilder<P> setStreet(final String street) {
      this.street = street;
      return this;
    }

    protected String getCity() {
      return city;
    }

    private PersonBuilder<P> setCity(final String city) {
      this.city = city;
      return this;
    }

    protected String getState() {
      return state;
    }

    private PersonBuilder<P> setState(final String state) {
      this.state = state;
      return this;
    }

    protected int getEducationLevel() {
      return educationLevel;
    }

    private PersonBuilder<P> setEducationLevel(final int educationLevel) {
      this.educationLevel = educationLevel;
      return this;
    }

    protected String getHighSchoolName() {
      return highSchoolName;
    }

    private PersonBuilder<P> setHighSchoolName(final String highSchoolName) {
      this.highSchoolName = highSchoolName;
      return this;
    }

    protected String getCollegeName() {
      return collegeName;
    }

    private PersonBuilder<P> setCollegeName(final String collegeName) {
      this.collegeName = collegeName;
      return this;
    }

    protected boolean isEnrolled() {
      return enrolled;
    }

    private PersonBuilder<P> setEnrolled(final boolean enrolled) {
      this.enrolled = enrolled;
      return this;
    }

    protected Date getDateOfJoining() {
      return dateOfJoining;
    }

    private PersonBuilder<P> setDateOfJoining(final Date dateOfJoining) {
      this.dateOfJoining = dateOfJoining;
      return this;
    }

    protected byte[] getRawData() {
      return rawData;
    }

    private PersonBuilder<P> setRawData(final byte[] rawData) {
      this.rawData = rawData;
      return this;
    }

    public boolean isResetFields() {
      return resetFields;
    }
  }
}
