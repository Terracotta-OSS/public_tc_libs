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

import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.UndeclaredThrowableException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;

/**
 * Provides access to US post-secondary (college, university, trade school) dat.
 * <p>
 * School data was obtained from <a href="http://www.ed.gov/developers">U.S. Department of Education: Developers</a>.
 *
 * @author Clifford W. Johnson
 */
public final class UsPostSecondaryData {
  private static final String US_POST_SECONDARY_DATA = "postscndryunivsrvy2013dirinfo.csv";

  private static final Map<UsState, List<UsPostSecondarySchool>> US_POST_SECONDARY_SCHOOLS_BY_STATE =
      Collections.unmodifiableMap(loadPostSecondarySchools());

  private static final NavigableMap<Integer, NavigableMap<Float, UsPostSecondarySchool>> US_POST_SECONDARY_SCHOOLS_BY_EDUCATION_LEVEL =
      calculateUsPostSecondarySchoolsByEducationLevel();

  /**
   * Private, niladic constructor to prevent instantiation.
   */
  private UsPostSecondaryData() {
  }

  /**
   * Gets a map of {@code UsPostSecondarySchool} instances by {@code UsState}.
   *
   * @return a map {@code UsPostSecondarySchool} instances grouped by state
   */
  public static Map<UsState, List<UsPostSecondarySchool>> getUsPostSecondarySchoolsByState() {
    return US_POST_SECONDARY_SCHOOLS_BY_STATE;
  }

  /**
   * Gets a map of maps grouping {@link UsPostSecondarySchool} instances, ordered by decreasing enrollment and
   * mapped by cumulative enrollment distribution, within education level.  In this collection, the
   * {@code UsPostSecondarySchool} instances are duplicated in each education level supported by a
   * {@code UsPostSecondarySchool}.
   * <p>
   * The cumulative enrollment distribution values are expressed in percentages ranging from 0.0 to 100.0.
   *
   * @return a map of maps grouping {@code UsPostSecondarySchool} instances by cumulative enrollment distribution
   *      within education level
   *
   * @see EducationLevel
   */
  public static NavigableMap<Integer, NavigableMap<Float, UsPostSecondarySchool>> getWeightedUsPostSecondarySchoolsByEducationLevel() {
    return US_POST_SECONDARY_SCHOOLS_BY_EDUCATION_LEVEL;
  }

  /**
   * Gets a randomly chosen post-secondary school supporting the education level specified.  The school
   * is chosen based on its enrollment size.
   *
   * @param rnd the {@code Random} instance to use for the choice
   * @param educationLevel the minimum education level for the school
   *
   * @return a randomly chosen post-secondary school providing at least {@code educationLevel} schooling
   *
   * @see EducationLevel
   */
  public static UsPostSecondarySchool getUsPostSecondarySchoolForEducationLevel(final Random rnd, final int educationLevel) {
    return US_POST_SECONDARY_SCHOOLS_BY_EDUCATION_LEVEL.floorEntry(educationLevel).getValue()
        .ceilingEntry(100 * rnd.nextFloat()).getValue();
  }

  /**
   * Loads the post-secondary school data from the resource <code>{@value #US_POST_SECONDARY_DATA}</code>
   * grouping the schools by state.
   *
   * @return a map grouping {@code UsPostSecondarySchool} instances by state
   */
  private static Map<UsState, List<UsPostSecondarySchool>> loadPostSecondarySchools() {
    final Map<String, UsState> usStatesByPostalCode = UsCensusData.getUsStatesByPostalCode();
    final long now = System.currentTimeMillis();

    final Map<UsState, List<UsPostSecondarySchool>> postSecondarySchools =
        new HashMap<UsState, List<UsPostSecondarySchool>>();

    String stateWithMostSchools = "";
    int maximumSchoolCount = 0;

    int universityCount = 0;
    InputStreamReader schoolsStream = null;
    CSVParser parser = null;
    try {
      schoolsStream =
          new InputStreamReader(UsPostSecondaryData.class.getResourceAsStream(US_POST_SECONDARY_DATA), Charset.forName("UTF-8"));
      parser = CSVFormat.DEFAULT.withHeader().parse(schoolsStream);
      for (final CSVRecord record : parser) {
        final UsState usState = usStatesByPostalCode.get(record.get("STABBR"));
        if (usState == null) {
          continue;
        }

        final int sector = Integer.parseInt(record.get("SECTOR"));
        if (sector < 1 || sector > 9) {
          // Weed out non-schools
          continue;
        }

        // INSTSIZE reports a "scale" of enrollment and not direct enrollment numbers
        final int enrollment;
        int instsize = Integer.parseInt(record.get("INSTSIZE"));
        if (instsize < 1 || instsize > 5) {
          continue;
        } else {
          enrollment = new int[] {500, 2500, 7500, 15000, 30000}[instsize - 1];
        }

        final int highestDegreeOffered;
        final String hloffer = record.get("HLOFFER");
        if (hloffer.isEmpty()) {
          // Did not report offering degrees
          continue;
        } else {
          final int hlofferInt = Integer.parseInt(hloffer);
          if (hlofferInt < 1 || hlofferInt > 9) {
            // Offers no degrees of interest
            continue;
          } else {
            highestDegreeOffered = new int[] {
                EducationLevel.POST_SECONDARY,   // 1    less than 1 year beyond high school
                EducationLevel.POST_SECONDARY,   // 2    At least 1, less than 2 years
                EducationLevel.ASSOCIATE_DEGREE, // 3    Associate's
                EducationLevel.ASSOCIATE_DEGREE, // 4    beyond 2, less than 4 years
                EducationLevel.BACHELOR_DEGREE,  // 5    Bachelor's
                EducationLevel.POST_GRADUATE,    // 6    post-baccalaureat
                EducationLevel.MASTER_DEGREE,    // 7    Master's
                EducationLevel.MASTER_DEGREE,    // 8    post-Master's
                EducationLevel.DOCTORAL_DEGREE   // 9    Doctor's
            }[hlofferInt - 1];
          }
        }

        final boolean ugoffer = (1 == Integer.parseInt(record.get("UGOFFER")));

        final String name = record.get("INSTNM");
        final String city = record.get("CITY");

        List<UsPostSecondarySchool> stateSchools = postSecondarySchools.get(usState);
        if (stateSchools == null) {
          stateSchools = new ArrayList<UsPostSecondarySchool>();
          postSecondarySchools.put(usState, stateSchools);
        }

        final UsPostSecondarySchool university = new UsPostSecondarySchool(name, usState, city, enrollment, highestDegreeOffered, ugoffer);
        universityCount++;
        stateSchools.add(university);

        if (stateSchools.size() > maximumSchoolCount) {
          maximumSchoolCount = stateSchools.size();
          stateWithMostSchools = usState.getName();
        }
      }
    } catch (IOException e) {
      throw new UndeclaredThrowableException(e, "Failed to load '" + US_POST_SECONDARY_DATA + "' resource");
    } finally {
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

    long elapsedTime = (System.currentTimeMillis() - now);
    //noinspection MalformedFormatString
    System.out.format("Loaded... %d post-secondary schools in %d states from %s in %ts.%<tL s%n",
        universityCount, postSecondarySchools.size(), US_POST_SECONDARY_DATA, elapsedTime);
    System.out.format("    %s has %d post-secondary schools%n", stateWithMostSchools, maximumSchoolCount);

    return postSecondarySchools;
  }

  /**
   * Scans the collection at {@link #US_POST_SECONDARY_SCHOOLS_BY_STATE} building a map grouping the
   * schools by (decreasing) cumulative enrollment and education level.
   *
   * @return a map grouping schools by education level and cumulative enrollment
   */
  private static NavigableMap<Integer, NavigableMap<Float, UsPostSecondarySchool>> calculateUsPostSecondarySchoolsByEducationLevel() {
    final NavigableMap<Integer, NavigableMap<Float, UsPostSecondarySchool>> postSecondarySchoolsByDegree =
        new TreeMap<Integer, NavigableMap<Float, UsPostSecondarySchool>>();

    final List<UsPostSecondarySchool> schools = new ArrayList<UsPostSecondarySchool>();
    for (final List<UsPostSecondarySchool> stateSchools : US_POST_SECONDARY_SCHOOLS_BY_STATE.values()) {
      schools.addAll(stateSchools);
    }
    /*
     * Sort full list of post-secondary schools by decreasing enrollment.
     */
    Collections.sort(schools, new Comparator<UsPostSecondarySchool>() {
      @Override
      public int compare(final UsPostSecondarySchool o1, final UsPostSecondarySchool o2) {
        return (o1.getEnrollment() == o2.getEnrollment() ? 0 : (o1.getEnrollment() > o2.getEnrollment() ? -1 : 1));
      }
    });

    final int[] postSecondaryEducationLevels = new int[] {
        EducationLevel.POST_SECONDARY,
        EducationLevel.ASSOCIATE_DEGREE,
        EducationLevel.BACHELOR_DEGREE,
        EducationLevel.POST_GRADUATE,
        EducationLevel.MASTER_DEGREE,
        EducationLevel.DOCTORAL_DEGREE
    };
    final Map<Integer, UsSchoolWeight.Builder> builders = new HashMap<Integer, UsSchoolWeight.Builder>();
    for (final int educationLevel : postSecondaryEducationLevels) {
      builders.put(educationLevel, new UsSchoolWeight.Builder());
    }

    /*
     * For each education level supported by the school, add the school to the cumulative distribution
     * builder for that level.
     */
    for (final UsPostSecondarySchool school : schools) {
      final int highestDegreeOffered = school.getHighestDegreeOffered();
      final boolean offersUndergraduateDegree = school.offersUndergraduateDegree();
      for (final int educationLevel : postSecondaryEducationLevels) {
        if (educationLevel <= highestDegreeOffered
            && (offersUndergraduateDegree || educationLevel > EducationLevel.BACHELOR_DEGREE)) {
          builders.get(educationLevel).weight(school, school.getEnrollment());
        }
      }
    }

    /*
     * Build the cumulative enrollment maps for each education level and add to the education level map.
     */
    for (final int educationLevel : postSecondaryEducationLevels) {
      final TreeMap<Float, UsPostSecondarySchool> schoolsByWeight = new TreeMap<Float, UsPostSecondarySchool>();
      for (final UsSchoolWeight weightedSchool : builders.get(educationLevel).build()) {
        schoolsByWeight.put(weightedSchool.getCumulativeWeight(), (UsPostSecondarySchool)weightedSchool.getValue());
      }
      postSecondarySchoolsByDegree.put(educationLevel, /* No unmodifiableNavigableMap in Java 6! */ schoolsByWeight);
    }

    return /* No unmodifiableNavigableMap in Java 6! */ postSecondarySchoolsByDegree;
  }
}
