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
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;

/**
 * Provides access to Google geolocation data.
 * <p>
 * A geolocation criteria file was obtained from
 * <a href="https://developers.google.com/adwords/api/docs/appendix/geotargeting">Google AdWords API - Geographical Targeting</a>.
 * <p>
 *
 * @author Clifford W. Johnson
 */
public final class GoogleGeolocationData {
  private static final String GOOGLE_GEOLOCATION_DATA = "cities.csv";

  private static final Map<Integer, LineItem> CRITERIA_MAP;
  private static final Map<String, List<LineItem>> COUNTRY_MAP;
  private static final Map<String, List<UsCity>> US_CITIES_BY_STATE_MAP;
  private static final List<UsCity> US_CITIES;
  static {
    final GoogleGeolocationLoader googleGeolocationLoader = new GoogleGeolocationLoader();
    CRITERIA_MAP = Collections.unmodifiableMap(googleGeolocationLoader.criteriaMap);
    COUNTRY_MAP = Collections.unmodifiableMap(googleGeolocationLoader.countryMap);

    final UsExtractor usExtractor = new UsExtractor(COUNTRY_MAP);
    US_CITIES = Collections.unmodifiableList(usExtractor.cities);
    US_CITIES_BY_STATE_MAP = Collections.unmodifiableMap(usExtractor.citiesByState);
  }

  /**
   * Private, niladic constructor to prevent instantiation.
   */
  private GoogleGeolocationData() {
  }

  /**
   * Gets the Criteria ID - to - geographical target map formed from the {@code cities.csv}
   * resource file.
   *
   * @return an unmodifiable {@code Map}
   */
  @SuppressWarnings("unused")
  public static Map<Integer, LineItem> getCriteriaMap() {
    return CRITERIA_MAP;
  }

  /**
   * Gets a map of US cities grouped by US state.
   *
   * @return an unmodifiable map of cities by state
   */
  public static Map<String, List<UsCity>> getUsCitiesByState() {
    return US_CITIES_BY_STATE_MAP;
  }

  /**
   * Get a list of US cities extracted from the raw data.
   *
   * @return an unmodifiable list of US cities
   */
  @SuppressWarnings("unused")
  public static List<UsCity> getUsCities() {
    return US_CITIES;
  }

  /**
   * Generates a random US city where the state selection weighted by US Census population.
   *
   * @param rnd the {@code Random} instance to use for the calculation
   *
   * @return a pseudo-random {@code UsCity}
   */
  public static UsCity chooseCity(final Random rnd) {

    /*
     * Choose a state based on population weighting and then some city within the state.
     */
    List<UsCity> cities;
    do {
      cities = getUsCitiesByState().get(UsCensusData.chooseState(rnd).toLowerCase(Locale.US));
    } while (cities == null);

    return cities.get(rnd.nextInt(cities.size()));
  }

  /**
   * Extractor for US-specific locations.
   */
  private static class UsExtractor {
    private Map<String, List<UsCity>> citiesByState;
    private List<UsCity> cities;

    private UsExtractor(final Map<String, List<LineItem>> countryMap) {
      final Map<String, List<UsCity>> citiesByState = new HashMap<String, List<UsCity>>();
      final List<UsCity> cities = new ArrayList<UsCity>();
      for (final LineItem item : countryMap.get("US")) {
        if ("State".equals(item.targetType)) {
          // Ensure state is present in US_STATE_MAP
          if (UsCensusData.getUsStateByName(item.name) == null) {
            throw new IllegalStateException(
                "Resource " + GOOGLE_GEOLOCATION_DATA + " contains an unrecognized US state name - " + item.name);
          }

        } else if ("City".equals(item.targetType)) {
          LineItem stateItem = getContainingState(item);
          if (stateItem == null) {
            continue;   // This city isn't in a state
          }
          final UsCity city = new UsCity(item.criteriaId, item.name, stateItem.name);
          cities.add(city);
          final String lowerState = stateItem.name.toLowerCase(Locale.US);
          List<UsCity> citiesInState = citiesByState.get(lowerState);
          if (citiesInState == null) {
            citiesInState = new ArrayList<UsCity>();
            citiesByState.put(lowerState, citiesInState);
          }
          citiesInState.add(city);
        }
      }

      System.out.format("    Identified %d US cities%n", cities.size());
      this.citiesByState = citiesByState;
      this.cities = cities;
    }

    /**
     * Traverses the Google geolocation data hierarchy attempting to find the US state
     * containing the location item provided.
     *
     * @param item the Google geolocation item to search
     *
     * @return the geolocation item for the containing state; {@code null} if {@code item}
     *        is not contained by a state
     */
    private static LineItem getContainingState(final LineItem item) {
      if ("State".equals(item.targetType)) {
        return item;
      }
      if (item.parentId == -1) {
        return null;
      }
      return getContainingState(CRITERIA_MAP.get(item.parentId));
    }
  }

  /**
   * Loads Google geolocation data from the <code>{@value #GOOGLE_GEOLOCATION_DATA}</code> resource file.
   */
  private static final class GoogleGeolocationLoader {

    private final Map<Integer, LineItem> criteriaMap;
    private final Map<String, List<LineItem>> countryMap;

    private GoogleGeolocationLoader() {
      long now = System.currentTimeMillis();

      final Map<Integer, LineItem> criteriaMap = new HashMap<Integer, LineItem>();
      final Map<String, List<LineItem>> countryMap = new HashMap<String, List<LineItem>>();

      final InputStreamReader citiesStream =
          new InputStreamReader(GoogleGeolocationData.class.getResourceAsStream(GOOGLE_GEOLOCATION_DATA), Charset.forName("UTF-8"));
      CSVParser parser = null;
      try {
        parser = CSVFormat.DEFAULT.withHeader().withSkipHeaderRecord().parse(citiesStream);
        for (final CSVRecord record : parser) {
          final LineItem lineItem = new LineItem(record);
          criteriaMap.put(lineItem.criteriaId, lineItem);

          List<LineItem> countryLineItems = countryMap.get(lineItem.countryCode);
          if (countryLineItems == null) {
            countryLineItems = new ArrayList<LineItem>();
            countryMap.put(lineItem.countryCode, countryLineItems);
          }
          countryLineItems.add(lineItem);
        }
      } catch (IOException e) {
        throw new UndeclaredThrowableException(e, "Failed to load '" + GOOGLE_GEOLOCATION_DATA + "' resource");
      } finally {
        if (parser != null) {
          try {
            parser.close();
          } catch (IOException e) {
            // ignored
          }
        }
        try {
          citiesStream.close();
        } catch (IOException e) {
          // ignored
        }
      }

      long elapsedTime = (System.currentTimeMillis() - now);
      //noinspection MalformedFormatString
      System.out.format("Loaded... %d locations from %s in %ts.%<tL s%n",
          criteriaMap.size(), GOOGLE_GEOLOCATION_DATA, elapsedTime);

      this.countryMap = countryMap;
      this.criteriaMap = criteriaMap;
    }
  }

  /**
   * Identifies a US item by name and state.
   */
  @SuppressWarnings("unused")
  private abstract static class UsItem {
    private final int criteriaId;
    private final String name;
    private final String state;

    private UsItem(final int criteriaId, final String name, final String state) {
      this.criteriaId = criteriaId;
      this.name = name;
      this.state = state;
    }

    public String getName() {
      return this.name;
    }

    public String getState() {
      return this.state;
    }
  }

  /**
   * Identifies a US city by name and state.
   */
  public static class UsCity extends UsItem {
    private UsCity(final int criteriaId, final String name, final String state) {
      super(criteriaId, name, state);
    }

    @Override
    public String toString() {
      return this.getName() + ", " + this.getState();
    }
  }

  /**
   * Describes a geographical location extracted from the {@value #GOOGLE_GEOLOCATION_DATA} resource file.  This
   * file is downloaded from
   * <a href="https://developers.google.com/adwords/api/docs/appendix/geotargeting">Google AdWords API - Geographical Targeting</a>.
   */
  @SuppressWarnings("unused")
  private static final class LineItem {
    private final int criteriaId;
    private final String name;
    private final String canonicalName;
    private final int parentId;
    private final String countryCode;
    private final String targetType;
    private final String status;

    private LineItem(final CSVRecord itemRecord) {
      this.criteriaId = Integer.parseInt(itemRecord.get(0));
      this.name = itemRecord.get(1);
      this.canonicalName = itemRecord.get(2);
      final String parentId = itemRecord.get(3);
      this.parentId = (parentId == null || parentId.isEmpty() ? -1 : Integer.parseInt(parentId));
      this.countryCode = itemRecord.get(4);
      this.targetType = itemRecord.get(5);
      this.status = itemRecord.get(6);
    }

    public int getCriteriaId() {
      return criteriaId;
    }

    public String getName() {
      return name;
    }

    public String getCanonicalName() {
      return canonicalName;
    }

    public int getParentId() {
      return parentId;
    }

    public String getCountryCode() {
      return countryCode;
    }

    public String getTargetType() {
      return targetType;
    }

    public String getStatus() {
      return status;
    }
  }
}
