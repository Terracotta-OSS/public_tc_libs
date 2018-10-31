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
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;

/**
 * Provides access to US primary and secondary school data.
 * <p>
 * School data was obtained from <a href="http://www.ed.gov/developers">U.S. Department of Education: Developers</a>.
 *
 * @author Clifford W. Johnson
 */
public final class UsK12SchoolData {
  private static final String US_PRIMARY_SECONDARY_SCHOOL_DATA =
    "userssharedsdfpublicelementarysecondaryunivsrvy200910.%d.csv.gz";
  private static final int PRIMARY_DATA_FILES_CNT = 10;

  private static final Map<UsState, List<UsSchool>> US_HIGH_SCHOOLS_BY_STATE =
      Collections.unmodifiableMap(loadHighSchools());

  private static final Map<UsState, NavigableMap<Float, UsSchool>> US_WEIGHTED_HIGH_SCHOOLS_BY_STATE =
      Collections.unmodifiableMap(calculateWeightedUsHighSchoolsByState());

  /**
   * Private, niladic constructor to prevent instantiation.
   */
  private UsK12SchoolData() {
  }

  /**
   * Gets a map of high schools (unordered) grouped by state.
   *
   * @return a map of high schools grouped by state
   */
  public static Map<UsState, List<UsSchool>> getUsHighSchoolsByState() {
    return US_HIGH_SCHOOLS_BY_STATE;
  }

  /**
   * Gets a map of high schools, ordered by decreasing enrollment and mapped by cumulative enrollment distribution,
   * within state.
   * <p>
   * The cumulative enrollment distribution values are expressed in percentages ranging from 0.0 to 100.0.
   *
   * @return a map of maps of high schools grouped by cumulative enrollment by state
   */
  public static Map<UsState, NavigableMap<Float, UsSchool>> getWeightedUsHighSchoolsByState() {
    return US_WEIGHTED_HIGH_SCHOOLS_BY_STATE;
  }

  /**
   * Gets a randomly chosen high school within the state indicated.  The high school is chosen
   * based on its enrollment size.
   *
   * @param rnd the {@code Random} instance to use for the choice
   * @param state the {@code UsState} instance indicating the state
   *
   * @return a randomly chosen high school in {@code state}
   */
  public static UsSchool getUsHighSchoolInState(final Random rnd, final UsState state) {
    return US_WEIGHTED_HIGH_SCHOOLS_BY_STATE.get(state).ceilingEntry(100 * rnd.nextFloat()).getValue();
  }

  /**
   * Extracts the US high school (schools having a 12th grade) information from
   * US Department of Education data.  Schools with no reported enrollment are
   * discarded.
   *
   * @return a list of high schools organized by state
   *
   * @see UsCensusData#getUsStatesByPostalCode()
   */
  @SuppressWarnings({ "ConstantConditions" })
  private static Map<UsState, List<UsSchool>> loadHighSchools() {
    final boolean debug = false;

    final Map<String, UsState> usStatesByPostalCode = UsCensusData.getUsStatesByPostalCode();
    final long now = System.currentTimeMillis();

    final Map<UsState, List<UsSchool>> highSchools = new LinkedHashMap<UsState, List<UsSchool>>();

    String stateWithMostSchools = "";
    int maximumSchoolCount = 0;

    int highSchoolCount = 0;
    InputStreamReader schoolsStream = null;
    CSVParser parser = null;
    for (int subFile = 0; subFile < PRIMARY_DATA_FILES_CNT; subFile++) {
      try {
        schoolsStream = new InputStreamReader(new GZIPInputStream(UsK12SchoolData.class.getResourceAsStream(
          String.format(US_PRIMARY_SECONDARY_SCHOOL_DATA, subFile))), Charset.forName("UTF-8"));

        parser = CSVFormat.DEFAULT.withHeader().parse(schoolsStream);
        for (final CSVRecord record : parser) {

          if (!record.isConsistent()) {
            System.out.format("Record %d/%d '%s' has only %d fields%n",
                              parser.getRecordNumber(),
                              parser.getCurrentLineNumber(),
                              record.get(0),
                              record.size());
            System.out.format("    %s%n", record);
            continue;
          }

          int schoolType = -1;
          int schoolLevel = -1;
          boolean badData = false;
          try {
            schoolType = Integer.parseInt(record.get("TYPE09"));
            schoolLevel = Integer.parseInt(record.get("LEVEL09"));
          } catch (NumberFormatException e) {
            badData = true;
          }
          if (badData || schoolType != 1 || schoolLevel != 3) {
            // Retain only "normal" high schools
            if (debug) {
              System.out.format("Dropping record %d/%d: wrong level%n", parser.getRecordNumber(), parser.getCurrentLineNumber());
            }
            continue;
          }

          final int enrollment = Integer.parseInt(record.get("MEMBER09"));
          if (enrollment <= 0) {
            // If enrollment wasn't reported, skip the school
            if (debug) {
              System.out.format("Dropping record %d/%d: no enrollment%n", parser.getRecordNumber(), parser.getCurrentLineNumber());
            }
            continue;
          }
          final String name = record.get("SCHNAM09");
          final String city = record.get("LCITY09");
          final UsState usState = usStatesByPostalCode.get(record.get("LSTATE09"));
          if (usState == null) {
            if (debug) {
              System.out.format("Dropping record %d/%d: not state%n", parser.getRecordNumber(), parser.getCurrentLineNumber());
            }
            continue;
          }
          List<UsSchool> stateSchools = highSchools.get(usState);
          if (stateSchools == null) {
            stateSchools = new ArrayList<UsSchool>();
            highSchools.put(usState, stateSchools);
          }
          final UsSchool usSchool = new UsSchool(name, usState, city, enrollment);
          stateSchools.add(usSchool);
          highSchoolCount++;
          if (stateSchools.size() > maximumSchoolCount) {
            maximumSchoolCount = stateSchools.size();
            stateWithMostSchools = usState.getName();
          }
          if (debug) {
            System.out.format("[%d] %d %s%n", highSchoolCount, parser.getRecordNumber(), usSchool);
          }
        }
      } catch(IOException e){
        throw new UndeclaredThrowableException(e, "Failed to load '" + US_PRIMARY_SECONDARY_SCHOOL_DATA + "' resource");
      } finally{
        if (parser != null) {
          try {
            parser.close();
          } catch (IOException e) {
            // ignored
          }
        }
        if (schoolsStream != null) {
          try {
            schoolsStream.close();
          } catch (IOException e) {
            // ignored
          }
        }
      }
    }
    long elapsedTime = (System.currentTimeMillis() - now);
    //noinspection MalformedFormatString
    System.out.format("Loaded... %d high schools in %d states from %s in %ts.%<tL s%n",
        highSchoolCount, highSchools.size(), US_PRIMARY_SECONDARY_SCHOOL_DATA, elapsedTime);
    System.out.format("    %s has %d high schools%n", stateWithMostSchools, maximumSchoolCount);

    return highSchools;
  }

  /**
   * Scans the collection at {@link #US_HIGH_SCHOOLS_BY_STATE} building a map grouping the
   * schools by (decreasing) cumulative enrollment and state.
   *
   * @return a map grouping schools by state and cumulative enrollment
   */
  private static Map<UsState, NavigableMap<Float, UsSchool>> calculateWeightedUsHighSchoolsByState() {
    final Map<UsState, NavigableMap<Float, UsSchool>> usStateHighSchools = new HashMap<UsState, NavigableMap<Float, UsSchool>>();
    for (final UsState state : UsCensusData.getUsStatesByName().values()) {
      final List<UsSchool> stateSchools = new ArrayList<UsSchool>(US_HIGH_SCHOOLS_BY_STATE.get(state));
      /*
       * Sort state schools by decreasing enrollment.
       */
      Collections.sort(stateSchools, new Comparator<UsSchool>() {
        @Override
        public int compare(final UsSchool o1, final UsSchool o2) {
          return (o1.getEnrollment() == o2.getEnrollment() ? 0 : (o1.getEnrollment() > o2.getEnrollment() ? -1 : 1));
        }
      });
      final UsSchoolWeight.Builder builder = new UsSchoolWeight.Builder();
      for (final UsSchool school : stateSchools) {
        builder.weight(school, school.getEnrollment());
      }
      final NavigableMap<Float, UsSchool> schoolsByWeight = new TreeMap<Float, UsSchool>();
      for (final UsSchoolWeight weightedSchool : builder.build()) {
        schoolsByWeight.put(weightedSchool.getCumulativeWeight(), weightedSchool.getValue());
      }
      usStateHighSchools.put(state, /* No unmodifiableNavigableMap in Java 6! */ schoolsByWeight);
    }

    return usStateHighSchools;
  }
}
