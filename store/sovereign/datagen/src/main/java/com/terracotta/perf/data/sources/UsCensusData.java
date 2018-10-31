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

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import com.terracotta.perf.data.EducationLevel;
import com.terracotta.perf.data.Gender;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.UndeclaredThrowableException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * Provides access to data obtained from US Census.
 * <p>
 * Name data files were obtained from
 * <a href="http://www.census.gov/topics/population/genealogy/data/1990_census/1990_census_namefiles.html">
 *   United States Census Bureau - <i>Frequently Occurring Surnames from Census 1990 – Names Files</i></a>
 *
 * @author Clifford W. Johnson
 */
public final class UsCensusData {

  private static final String US_CENSUS_NATIONAL_TOTALS_DATA = "NST-EST2015-01.csv";
  private static final String US_CENSUS_SURNAME_DATA = "dist.all.last";
  private static final String US_CENSUS_FEMALE_NAME_DATA = "dist.female.first";
  private static final String US_CENSUS_MALE_NAME_DATA = "dist.male.first";

  /**
   * The percentage of the US population recorded as male.
   * Derived from <a href="http://www2.census.gov/census_2000/datasets/demographic_profile/0_United_States/2kh00.pdf">
   *    Profile of General Demographic Characteristics: 2000</a>
   */
  private static final float MALE_GENDER_PERCENTAGE = 49.1F;

  /**
   * US age distribution to use for person instances.
   * Derived from <a href="http://www2.census.gov/census_2000/datasets/demographic_profile/0_United_States/2kh00.pdf">
   *   Profiles of General Demographic Characteristics: 2000</a>
   */
  // Published distribution percentages are used; they are normalized by AgeDistribution.Builder to accumulate to 100.0%
  private static final AgeDistribution BIRTH_AGE_DISTRIBUTION =
      new AgeDistribution.Builder()
          .range(18, 19, 2.9F)        // Extrapolated: actual range is 15 to 19 years at 7.2%
          .range(20, 24, 6.7F)
          .range(25, 34, 14.2F)
          .range(35, 44, 16.0F)
          .range(45, 54, 13.4F)
          .range(55, 59, 4.8F)
          .range(60, 64, 3.8F)
          .range(65, 74, 6.5F)
          .build();

  /**
   * Education level distribution.  These figures are extracted from the
   * <i>Population over 25 years old and over</i> category from the 2014 data available via
   * <a href="http://factfinder.census.gov/faces/tableservices/jsf/pages/productview.xhtml?pid=ACS_14_5YR_S1501&src=pt">
   *   American Community Survey (ACS) - 2010-2014 American Community Survey 5-Year Estimates - S1501 - EDUCATIONAL ATTAINMENT</a>
   * table.  This survey does not break down degrees earned beyond Bachelor's degrees and does not identify
   * post-graduate, non-degree work or post-doctoral work -- these are all classified as Master's degrees in this
   * table.  This survey groups all with a less than a 9th grade education into a single category -- these are
   * classified as 8 years of education in this table.
   */
  private static final List<EducationLevelWeight> EDUCATION_LEVEL_DISTRIBUTION;
  static {
    EDUCATION_LEVEL_DISTRIBUTION = Collections.unmodifiableList(
        new EducationLevelWeight.Builder()
            .weight(8, 5.8F)
            .weight(11, 7.8F)
            .weight(EducationLevel.HIGH_SCHOOL_GRAD, 28.0F)
            .weight(EducationLevel.POST_SECONDARY, 21.2F)
            .weight(EducationLevel.ASSOCIATE_DEGREE, 7.9F)
            .weight(EducationLevel.BACHELOR_DEGREE, 18.3F)
            .weight(EducationLevel.MASTER_DEGREE, 11.0F)
            .build());
  }

  /**
   * Maps the US state name to the ANSI state code and USPS state abbreviation.
   * This map is indexed by the lower case state name to permit lookups from
   * the Google geolocation data.
   * <p>
   * Source: <a href="https://www.census.gov/geo/reference/ansi_statetables.html">
   *   American National Standards Institute (ANSI) Codes for States, the District of Columbia, Puerto Rico,
   *   and the Insular Areas of the United States</a>
   */
  private static final Map<String, UsState> US_STATES_BY_NAME;
  private static final Map<String, UsState> US_STATES_BY_POSTAL_CODE;
  static {
    final Map<String, UsState> statesByName = new HashMap<String, UsState>();
    final Map<String, UsState> statesByPostalCode = new HashMap<String, UsState>();
    addState(statesByName, statesByPostalCode, "Alabama", 1, "AL");
    addState(statesByName, statesByPostalCode, "Alaska", 2, "AK");
    addState(statesByName, statesByPostalCode, "Arizona", 4, "AZ");
    addState(statesByName, statesByPostalCode, "Arkansas", 5, "AR");
    addState(statesByName, statesByPostalCode, "California", 6, "CA");
    addState(statesByName, statesByPostalCode, "Colorado", 8, "CO");
    addState(statesByName, statesByPostalCode, "Connecticut", 9, "CT");
    addState(statesByName, statesByPostalCode, "Delaware", 10, "DE");
    addState(statesByName, statesByPostalCode, "District of Columbia", 11, "DC");
    addState(statesByName, statesByPostalCode, "Florida", 12, "FL");
    addState(statesByName, statesByPostalCode, "Georgia", 13, "GA");
    addState(statesByName, statesByPostalCode, "Hawaii", 15, "HI");
    addState(statesByName, statesByPostalCode, "Idaho", 16, "ID");
    addState(statesByName, statesByPostalCode, "Illinois", 17, "IL");
    addState(statesByName, statesByPostalCode, "Indiana", 18, "IN");
    addState(statesByName, statesByPostalCode, "Iowa", 19, "IA");
    addState(statesByName, statesByPostalCode, "Kansas", 20, "KS");
    addState(statesByName, statesByPostalCode, "Kentucky", 21, "KY");
    addState(statesByName, statesByPostalCode, "Louisiana", 22, "LA");
    addState(statesByName, statesByPostalCode, "Maine", 23, "ME");
    addState(statesByName, statesByPostalCode, "Maryland", 24, "MD");
    addState(statesByName, statesByPostalCode, "Massachusetts", 25, "MA");
    addState(statesByName, statesByPostalCode, "Michigan", 26, "MI");
    addState(statesByName, statesByPostalCode, "Minnesota", 27, "MN");
    addState(statesByName, statesByPostalCode, "Mississippi", 28, "MS");
    addState(statesByName, statesByPostalCode, "Missouri", 29, "MO");
    addState(statesByName, statesByPostalCode, "Montana", 30, "MT");
    addState(statesByName, statesByPostalCode, "Nebraska", 31, "NE");
    addState(statesByName, statesByPostalCode, "Nevada", 32, "NV");
    addState(statesByName, statesByPostalCode, "New Hampshire", 33, "NH");
    addState(statesByName, statesByPostalCode, "New Jersey", 34, "NJ");
    addState(statesByName, statesByPostalCode, "New Mexico", 35, "NM");
    addState(statesByName, statesByPostalCode, "New York", 36, "NY");
    addState(statesByName, statesByPostalCode, "North Carolina", 37, "NC");
    addState(statesByName, statesByPostalCode, "North Dakota", 38, "ND");
    addState(statesByName, statesByPostalCode, "Ohio", 39, "OH");
    addState(statesByName, statesByPostalCode, "Oklahoma", 40, "OK");
    addState(statesByName, statesByPostalCode, "Oregon", 41, "OR");
    addState(statesByName, statesByPostalCode, "Pennsylvania", 42, "PA");
    addState(statesByName, statesByPostalCode, "Rhode Island", 44, "RI");
    addState(statesByName, statesByPostalCode, "South Carolina", 45, "SC");
    addState(statesByName, statesByPostalCode, "South Dakota", 46, "SD");
    addState(statesByName, statesByPostalCode, "Tennessee", 47, "TN");
    addState(statesByName, statesByPostalCode, "Texas", 48, "TX");
    addState(statesByName, statesByPostalCode, "Utah", 49, "UT");
    addState(statesByName, statesByPostalCode, "Vermont", 50, "VT");
    addState(statesByName, statesByPostalCode, "Virginia", 51, "VA");
    addState(statesByName, statesByPostalCode, "Washington", 53, "WA");
    addState(statesByName, statesByPostalCode, "West Virginia", 54, "WV");
    addState(statesByName, statesByPostalCode, "Wisconsin", 55, "WI");
    addState(statesByName, statesByPostalCode, "Wyoming", 56, "WY");
    US_STATES_BY_NAME = Collections.unmodifiableMap(statesByName);
    US_STATES_BY_POSTAL_CODE = Collections.unmodifiableMap(statesByPostalCode);
  }

  /**
   * Private, niladic constructor to prevent instantiation.
   */
  private UsCensusData() {
  }

  /**
   * Gets an {@link AgeDistribution} instance for the selection of birth ages.
   */
  public static AgeDistribution getBirthAgeDistribution() {
    return BIRTH_AGE_DISTRIBUTION;
  }

  /**
   * Gets the list of {@link WeightedValue} instances ordered by cumulative distribution describing
   * the education level distribution.
   */
  @SuppressWarnings("unchecked")
  public static List<WeightedValue<Integer>> getEducationLevelDistribution() {
    return (List<WeightedValue<Integer>>)(Object)EDUCATION_LEVEL_DISTRIBUTION;    // Unchecked
  }

  /**
   * Gets a list of {@link StatePopulationWeight} instances ordered by cumulative distribution.
   */
  public static List<StatePopulationWeight> getStatePopulationDistribution() {
    return StatePopulationLoader.STATE_POPULATION_DISTRIBUTION;
  }

  /**
   * Gets a map of US state to its {@link StatePopulation} instance.
   *
   * @return an unmodifiable map
   */
  @SuppressWarnings("unused")
  public static Map<String, StatePopulation> getUsStatePopulationMap() {
    return StatePopulationLoader.US_STATE_POPULATION_MAP;
  }

  /**
   * Get a map of US surnames by cumulative distribution percentage extracted from the raw data.
   * The highest cumulative distribution value will not be 100.0; to ensure anonymous data, the
   * US Census Bureau elides names whose frequency in a census is below a certain threshold.
   *
   * @return a map of US surnames by distribution
   */
  public static NavigableMap<Float, List<CensusName>> getUsSurnames() {
    return SurnameLoader.US_SURNAMES;
  }

  /**
   * Get a map of US female names by cumulative distribution percentage extracted from the raw data.
   * The highest cumulative distribution value will not be 100.0; to ensure anonymous data, the
   * US Census Bureau elides names whose frequency in a census is below a certain threshold.
   *
   * @return a map of US female names by distribution
   */
  public static NavigableMap<Float, List<CensusName>> getUsFemaleNames() {
    return FemaleNameLoader.US_FEMALE_NAMES;
  }

  /**
   * Get a map of US male names by cumulative distribution percentage extracted from the raw data.
   * The highest cumulative distribution value will not be 100.0; to ensure anonymous data, the
   * US Census Bureau elides names whose frequency in a census is below a certain threshold.
   *
   * @return a map of US male names by distribution
   */
  public static NavigableMap<Float, List<CensusName>> getUsMaleNames() {
    return MaleNameLoader.US_MALE_NAMES;
  }

  /**
   * Choose a pseudo-random surname from the US Census data based on the distribution from the
   * census data.
   *
   * @param rnd a {@code Random} instance to use for the choice
   *
   * @return a surname chosen from the census data
   */
  public static String chooseSurname(final Random rnd) {
    return chooseName(rnd, SurnameLoader.US_SURNAMES, SurnameLoader.US_SURNAMES_MAX_CUMULATIVE_DISTRIBUTION);
  }

  /**
   * Choose a pseudo-random female from the US Census data based on the distribution from the
   * census data.
   *
   * @param rnd a {@code Random} instance to use for the choice
   *
   * @return a female name chosen from the census data
   */
  public static String chooseFemaleName(final Random rnd) {
    return chooseName(rnd, FemaleNameLoader.US_FEMALE_NAMES, FemaleNameLoader.US_FEMALE_NAMES_MAX_CUMULATIVE_DISTRIBUTION);
  }

  /**
   * Choose a pseudo-random male name from the US Census data based on the distribution from the
   * census data.
   *
   * @param rnd a {@code Random} instance to use for the choice
   *
   * @return a male name chosen from the census data
   */
  public static String chooseMaleName(final Random rnd) {
    return chooseName(rnd, MaleNameLoader.US_MALE_NAMES, MaleNameLoader.US_MALE_NAMES_MAX_CUMULATIVE_DISTRIBUTION);
  }

  /**
   * Gets a map of US state identification by lower-case state name.
   */
  public static Map<String, UsState> getUsStatesByName() {
    return US_STATES_BY_NAME;
  }

  /**
   * Gets the {@code UsState} for the state name provided.
   *
   * @param stateName the state name
   *
   * @return the {@code UsState} for {@code stateName}
   */
  public static UsState getUsStateByName(final String stateName) {
    return US_STATES_BY_NAME.get(stateName.toLowerCase(Locale.US));
  }

  /**
   * Chooses a random US state weighted by population.
   *
   * @param rnd the {@code Random} instance to use for the selection
   *
   * @return the name of the selected state
   */
  public static String chooseState(final Random rnd) {
    final float choice = 100.0F * rnd.nextFloat();
    String stateName;
    haveState: {
      for (final StatePopulationWeight stateWeight : StatePopulationLoader.STATE_POPULATION_DISTRIBUTION) {
        if (choice <= stateWeight.getCumulativeWeight()) {
          stateName = stateWeight.getValue().getName();
          break haveState;
        }
      }
      stateName = StatePopulationLoader.STATE_POPULATION_DISTRIBUTION.get(0).getValue().getName();
    }
    return stateName;
  }

  /**
   * Gets a map of the US state identification by postal code (abbreviation).
   *
   * @return an unmodifiable map
   */
  public static Map<String, UsState> getUsStatesByPostalCode() {
    return US_STATES_BY_POSTAL_CODE;
  }

  /**
   * Returns an education level based on the distribution in {@link UsCensusData#getEducationLevelDistribution()}.
   *
   * @param rnd the {@code Random} instance to use for education level selection
   *
   * @return an education level
   */
  public static int chooseEducationLevel(final Random rnd) {
    final float choice = 100.0F * rnd.nextFloat();
    for (final EducationLevelWeight educationLevelWeight : EDUCATION_LEVEL_DISTRIBUTION) {
      if (choice <= educationLevelWeight.getCumulativeWeight()) {
        return educationLevelWeight.getValue();
      }
    }
    return EducationLevel.POST_DOCTORAL;
  }

  /**
   * Returns a gender based on US Census Bureau distribution.  The distribution used
   * was obtained from the 2010 census.
   *
   * @param rnd the {@code Random} instance to use for the selection
   *
   * @return a {@link Gender} constant
   */
  public static Gender chooseGender(final Random rnd) {
    return (weightedTrue(rnd, MALE_GENDER_PERCENTAGE) ? Gender.MALE : Gender.FEMALE);
  }

  /**
   * Loads the US state population data from <code>{@value #US_CENSUS_NATIONAL_TOTALS_DATA}</code>.
   *
   * @return the map of lower-case state name to {@link StatePopulation} instance
   */
  private static Map<String, StatePopulation> loadStatePopulation() {

    final Map<String, StatePopulation> statePopulationMap = new LinkedHashMap<String, StatePopulation>();

    final InputStreamReader nameStream =
        new InputStreamReader(GoogleGeolocationData.class.getResourceAsStream(US_CENSUS_NATIONAL_TOTALS_DATA), Charset.forName("UTF-8"));
    final BufferedReader bufferedReader = new BufferedReader(nameStream);
    CSVParser parser = null;
    try {
      // Skip the first three lines -- unmarked comments
      for (int i = 0; i < 3; i++) {
        bufferedReader.readLine();
      }
      parser = CSVFormat.DEFAULT
          .withHeader()
          .withAllowMissingColumnNames()
          .withIgnoreEmptyLines()
          .parse(bufferedReader);
      for (final CSVRecord record : parser) {
        String stateName = record.get(0);
        if (stateName.startsWith(".")) {
          stateName = stateName.substring(1);
          if (!getUsStatesByName().containsKey(stateName.toLowerCase(Locale.US))) {
            throw new IllegalStateException(
                "Resource '" + US_CENSUS_NATIONAL_TOTALS_DATA + "' contains unrecognized state name - " + stateName);
          }
          final int population = Integer.parseInt(record.get("Census").replace(",", ""));
          statePopulationMap.put(stateName.toLowerCase(Locale.US), new StatePopulation(stateName, population));
        }
      }
    } catch (IOException e) {
      throw new UndeclaredThrowableException(e, "Failed to load '" + US_CENSUS_NATIONAL_TOTALS_DATA + "' resource");
    } finally {
      if (parser != null) {
        try {
          parser.close();
        } catch (IOException e) {
          // ignored
        }
      }
      try {
        bufferedReader.close();
      } catch (IOException e) {
        // ignored
      }
    }

    return statePopulationMap;
  }

  /**
   * Extracts names from a US Census name data file.  Four whitespace-separated tokens per line are
   * expected: name, distribution percentage, cumulative distribution percentage, rank.  Multiple
   * entries may have the same cumulative distribution percentage but each rank must be unique.
   *
   * @param resourceName the name of the resource file containing the names
   *
   * @return a {@code NavigableMap} mapping the cumulative distribution to a list of {@link CensusName} instances
   *        having that cumulative distribution
   */
  private static NavigableMap<Float, List<CensusName>> loadNames(final String resourceName) {
    final long now = System.currentTimeMillis();

    int nameCount = 0;
    final NavigableMap<Float, List<CensusName>> namesByDistribution = new TreeMap<Float, List<CensusName>>();

    final Pattern separator = Pattern.compile("\\s+");
    final InputStreamReader nameStream =
        new InputStreamReader(GoogleGeolocationData.class.getResourceAsStream(resourceName), Charset.forName("UTF-8"));
    final BufferedReader bufferedReader = new BufferedReader(nameStream);
    try {
      String record;
      while ((record = bufferedReader.readLine()) != null) {
        final String[] tokens = separator.split(record);
        if (tokens.length <= 1) {
          continue;
        }

        /*
         * The names lists have cumulative distribution value collisions; use a List to
         * group the colliding entries.
         */
        final CensusName censusName = new CensusName(tokens);
        List<CensusName> nameList = namesByDistribution.get(censusName.cumulativeDistribution);
        if (nameList == null) {
          nameList = new ArrayList<CensusName>(1);
          namesByDistribution.put(censusName.cumulativeDistribution, nameList);
        }
        nameCount++;
        nameList.add(censusName);
      }
    } catch (IOException e) {
      throw new UndeclaredThrowableException(e, "Failed to load '" + resourceName + "' resource");
    } finally {
      try {
        bufferedReader.close();
      } catch (IOException e) {
        // ignored
      }
    }

    long elapsedTime = (System.currentTimeMillis() - now);
    //noinspection MalformedFormatString
    System.out.format("Loaded... %d names into %d buckets from %s in %ts.%<tL s%n",
        nameCount, namesByDistribution.size(), resourceName, elapsedTime);

    return namesByDistribution;
  }

  /**
   * Adds a new {@link UsState} to the maps provided.
   *
   * @param statesByName the map of lower-case state name to {@code UsState}
   * @param statesByPostalCode the map of state postal code to {@code UsState}
   * @param name the state name
   * @param ansiCode the ANSI state code
   * @param postalCode the US Postal Service state abbreviation
   */
  private static void addState(final Map<String, UsState> statesByName,
                                  final Map<String, UsState> statesByPostalCode,
                                  final String name, final int ansiCode, final String postalCode) {
    final UsState state = new UsState(name, (short)ansiCode, postalCode);
    if (statesByName.put(name.toLowerCase(Locale.US), state) != null) {
      throw new IllegalStateException();
    }
    if (statesByPostalCode.put(postalCode, state) != null) {
      throw new IllegalStateException();
    }
  }

  /**
   * Returns a name from the US Census name list provided.
   *
   * @param rnd the {@code Random} instance to use for name selection
   * @param nameMap the list of {@code CensusName} instances from which the name is chosen;
   *              this list must be ordered by rank
   * @param maxCumulativeDistribution the maximum cumulative distribution present in
   *              {@code nameMap}; should be {@link NavigableMap#lastKey() nameMap.lastKey()}
   *
   * @return a name from {@code names}
   */
  private static String chooseName(final Random rnd,
                                   final NavigableMap<Float, List<CensusName>> nameMap,
                                   final float maxCumulativeDistribution) {
    final float choice = maxCumulativeDistribution * rnd.nextFloat();

    final List<CensusName> censusNames = nameMap.ceilingEntry(choice).getValue();
    final CensusName censusName;
    if (censusNames != null) {
      if (censusNames.size() == 1) {
        censusName = censusNames.get(0);
      } else {
        censusName = censusNames.get(rnd.nextInt(censusNames.size()));
      }
    } else {
      censusName = nameMap.firstEntry().getValue().get(0);
    }

    return censusName.getName();
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
  private static boolean weightedTrue(final Random rnd, final float weight) {
    return (100.0F * rnd.nextFloat() <= weight);
  }

  /**
   * Associates a state name with its census population.
   */
  public static final class StatePopulation {
    private final String name;
    private final int population;

    private StatePopulation(final String name, final int population) {
      this.name = name;
      this.population = population;
    }

    /**
     * Gets the state name.
     */
    public String getName() {
      return name;
    }

    /**
     * Gets the population.
     */
    public int getPopulation() {
      return population;
    }

    @Override
    public String toString() {
      return "StatePopulation{" +
          "name='" + name + '\'' +
          ", population=" + population +
          '}';
    }
  }

  /**
   * Describes the distribution weight for a {@link StatePopulation StatePopulation} value.
   * The state's population is used as the weight.
   */
  public static final class StatePopulationWeight extends WeightedValue<StatePopulation> {
    public StatePopulationWeight(final StatePopulation value, final float weight) {
      super(value, weight);
    }

    private static final class Builder extends WeightedValue.Builder<StatePopulation, StatePopulationWeight> {
      @Override
      protected StatePopulationWeight newInstance(final StatePopulation value, final float weight) {
        return new StatePopulationWeight(value, weight);
      }
    }
  }

  /**
   * Describes the distribution weight for a {@link EducationLevel EducationLevel} value.
   */
  private static final class EducationLevelWeight extends WeightedValue<Integer> {
    private EducationLevelWeight(final int educationLevel, final float weight) {
      super(educationLevel, weight);
    }

    private static final class Builder extends WeightedValue.Builder<Integer, EducationLevelWeight> {
      @Override
      protected EducationLevelWeight newInstance(final Integer value, final float weight) {
        return new EducationLevelWeight(value, weight);
      }
    }
  }

  /**
   * Represents a US Census name record.  The original record format is described at
   * <a href="http://www2.census.gov/topics/genealogy/1990surnames/nam_meth.txt">
   *   DOCUMENTATION AND METHODOLOGY FOR FREQUENTLY OCCURRING NAMES IN THE U.S.--1990</a>.
   * <p>
   * The "natural" ordering for {@code CensusName} instances is by rank.
   */
  @SuppressWarnings("unused")
  public static final class CensusName implements Comparable<CensusName> {
    private final String name;
    private final float distribution;
    private final float cumulativeDistribution;
    private final int rank;

    private CensusName(final String[] tokens) throws NumberFormatException {
      this.name = tokens[0];
      this.distribution = Float.parseFloat(tokens[1]);
      this.cumulativeDistribution = Float.parseFloat(tokens[2]);
      this.rank = Integer.parseInt(tokens[3]);
    }

    public String getName() {
      return name;
    }

    public float getDistribution() {
      return distribution;
    }

    public float getCumulativeDistribution() {
      return cumulativeDistribution;
    }

    public int getRank() {
      return rank;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      final CensusName that = (CensusName)o;
      return Float.compare(that.distribution, distribution) == 0 &&
          Float.compare(that.cumulativeDistribution, cumulativeDistribution) == 0 && rank == that.rank &&
          name.equals(that.name);
    }

    @Override
    public int hashCode() {
      int result = name.hashCode();
      result = 31 * result + (distribution != +0.0f ? Float.floatToIntBits(distribution) : 0);
      result = 31 * result + (cumulativeDistribution != +0.0f ? Float.floatToIntBits(cumulativeDistribution) : 0);
      result = 31 * result + rank;
      return result;
    }

    /**
     * {@inheritDoc}
     * This implementation orders {@code CensusName} instances by ascending rank.
     * @param that {@inheritDoc}
     * @return {@inheritDoc}
     */
    @Override
    public int compareTo(@SuppressWarnings("NullableProblems") final CensusName that) {
      if (that == null) {
        throw new NullPointerException();
      }
      return (this.rank == that.rank ? 0 : (this.rank > that.rank ? 1 : -1));
    }
  }

  /**
   * Internal class to support load on first reference to US Census state population information.
   */
  private static final class StatePopulationLoader {

    private static final Map<String, StatePopulation> US_STATE_POPULATION_MAP =
        Collections.unmodifiableMap(loadStatePopulation());

    /**
     * The US states distribution based on population.
     */
    private static final List<StatePopulationWeight> STATE_POPULATION_DISTRIBUTION;
    static {
      final UsCensusData.StatePopulationWeight.Builder builder = new UsCensusData.StatePopulationWeight.Builder();

      final List<StatePopulation> statePopulationList =
          new ArrayList<StatePopulation>(US_STATE_POPULATION_MAP.values());
      Collections.sort(statePopulationList, new Comparator<StatePopulation>() {
        /**
         * Comparator to sort {@code StatePopulation} in descending order of population.
         */
        @Override
        public int compare(final StatePopulation o1, final StatePopulation o2) {
          return (o1.getPopulation() == o2.getPopulation()) ? 0 : ((o1.getPopulation() > o2.getPopulation()) ? -1 : 1);
        }
      });
      for (final StatePopulation statePopulation : statePopulationList) {
        builder.weight(statePopulation, statePopulation.getPopulation());
      }

      STATE_POPULATION_DISTRIBUTION = Collections.unmodifiableList(builder.build());
    }
  }

  /**
   * Internal class to support load on first reference to US Census surname data.
   */
  private static final class SurnameLoader {
    private static final NavigableMap<Float, List<CensusName>> US_SURNAMES = loadNames(US_CENSUS_SURNAME_DATA);
    private static final float US_SURNAMES_MAX_CUMULATIVE_DISTRIBUTION = US_SURNAMES.lastKey();
  }

  /**
   * Internal class to support load on first reference to US Census female name data.
   */
  private static final class FemaleNameLoader {
    private static final NavigableMap<Float, List<CensusName>> US_FEMALE_NAMES = loadNames(US_CENSUS_FEMALE_NAME_DATA);
    private static final float US_FEMALE_NAMES_MAX_CUMULATIVE_DISTRIBUTION = US_FEMALE_NAMES.lastKey();
  }

  /**
   * Internal class to support load on first reference to US Census male name data.
   */
  private static final class MaleNameLoader {
    private static final NavigableMap<Float, List<CensusName>> US_MALE_NAMES = loadNames(US_CENSUS_MALE_NAME_DATA);
    private static final float US_MALE_NAMES_MAX_CUMULATIVE_DISTRIBUTION = US_MALE_NAMES.lastKey();
  }
}
