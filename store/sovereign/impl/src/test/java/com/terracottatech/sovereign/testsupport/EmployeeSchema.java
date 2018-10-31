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
package com.terracottatech.sovereign.testsupport;

import com.terracottatech.store.definition.StringCellDefinition;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.Record;
import com.terracottatech.store.Type;
import com.terracottatech.store.definition.DoubleCellDefinition;
import com.terracottatech.store.definition.IntCellDefinition;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Class that is commonly shared to load and process the employee schema from the csv file
 * by multiple tests.
 *
 * @author RKAV
 */
public final class EmployeeSchema {
  // load input data only once..Change these values if new records are added to the CSV
  public static final int MAX_EMPLOYEE_RECORDS = 10000; // this should exactly match number of records in csv
  public static final String LAST_STATE_IN_CSV = "Wyoming"; // this should match the alphabetically last in csv

  // cell definitions, change if more cell definitions are added to the csv
  public static final IntCellDefinition idx = CellDefinition.defineInt("empId");
  public static final CellDefinition<String> firstName = CellDefinition.define("firstName", Type.STRING);
  public static final CellDefinition<String> lastName = CellDefinition.define("lastName", Type.STRING);
  public static final CellDefinition<String> email = CellDefinition.define("Email", Type.STRING);
  public static final DoubleCellDefinition salary = CellDefinition.defineDouble("Salary");
  public static final IntCellDefinition age = CellDefinition.defineInt("Age");
  public static final CellDefinition<String> state = CellDefinition.define("State", Type.STRING);
  public static final StringCellDefinition country = CellDefinition.defineString("Country");
  public static final DoubleCellDefinition weight = CellDefinition.defineDouble("Weight");
  public static final CellDefinition<Boolean> married = CellDefinition.define("Married", Type.BOOL);
  public static final CellDefinition<String> phone = CellDefinition.define("Phone", Type.STRING);
  public static final CellDefinition<byte[]> photo = CellDefinition.define("Photo", Type.BYTES);

  // create a convenient "name" to "cell definition" mapping for all the cell definition
  public static final Map<String, CellDefinition<?>> employeeCellDefinitionMap =
      Arrays.stream(new CellDefinition<?>[] {
          firstName,
          lastName,
          email,
          salary,
          age,
          state,
          country,
          weight,
          married,
          phone,
          photo
      }).collect(Collectors.toMap(CellDefinition::name, c -> c));

  // predicates with cell functions, a.k.a comparison predicates
  public static final Predicate<Record<?>> idxRange100to500 =
      idx.value().isLessThan(500).and(idx.value().isGreaterThan(100));
  public static final Predicate<Record<?>> idxRange1000to5000 =
      idx.value().isLessThan(5000).and(idx.value().isGreaterThan(1000));
  public static final Predicate<Record<?>> idxRangeLast10 =
      idx.value().isGreaterThanOrEqualTo(MAX_EMPLOYEE_RECORDS - 10);
  public static final Predicate<Record<?>> idxRange9990to9999 =
      idx.value().isGreaterThanOrEqualTo(9990).and(idx.value().isLessThanOrEqualTo(9999));
  public static final Predicate<Record<?>> idxRangeFirst10 = idx.value().isLessThanOrEqualTo(9);
  public static final Predicate<Record<?>> idxFirst = idx.value().is(0);
  public static final Predicate<Record<?>> idxLast = idx.value().is(MAX_EMPLOYEE_RECORDS - 1);
  public static final Predicate<Record<?>> idxRange2500 = idx.value().isLessThan(2500);
  public static final Predicate<Record<?>> idxRangeFirst = idx.value().isGreaterThanOrEqualTo(0);
  public static final Predicate<Record<?>> idxRange5000 = idx.value().isLessThanOrEqualTo(4999);
  public static final Predicate<Record<?>> idxRangeLast = idx.value().isLessThan(MAX_EMPLOYEE_RECORDS);

  // predicates with normal lambdas
  public static final Predicate<Record<?>> recordIdxRange100to500 = (r) -> (r.get(idx).get() > 100 && r.get(idx).get() < 500);
  public static final Predicate<Record<?>> recordIdxRange1000to5000 = (r) -> (r.get(idx).get() > 1000 && r.get(idx).get() < 5000);
  public static final Predicate<Record<?>> recordIdxRangeLast10 = (r) -> (r.get(idx).get() >= MAX_EMPLOYEE_RECORDS - 10);
  public static final Predicate<Record<?>> recordIdxRange9990to9999 = (r) -> (r.get(idx).get() >= 9990 && r.get(idx).get() <= 9999);
  public static final Predicate<Record<?>> recordIdxRangeFirst10 = (r) -> (r.get(idx).get() <= 9);
  public static final Predicate<Record<?>> recordIdxFirst = (r) -> (r.get(idx).get() == 0);
  public static final Predicate<Record<?>> recordIdxLastMinus10 = (r) -> (r.get(idx).get() == MAX_EMPLOYEE_RECORDS - 10);
  public static final Predicate<Record<?>> recordIdxLastMinus5 = (r) -> (r.get(idx).get() == MAX_EMPLOYEE_RECORDS - 5);
  public static final Predicate<Record<?>> recordIdxRangeFirst = (r) -> (r.get(idx).get() >= 0);
  public static final Predicate<Record<?>> recordIdxRange5000 = (r) -> (r.get(idx).get() <= 4999);
  public static final Predicate<Record<?>> recordIdxRangeLast = (r) -> (r.get(idx).get() < MAX_EMPLOYEE_RECORDS);

  /**
   * Load the Data from the CSV file into a List of hash maps.
   *
   * @throws java.io.IOException if any file error occurs..
   */
  public static List<Map<String, Object>> loadData() throws IOException {
    List<Map<String, Object>> loadedData = new ArrayList<>();
    try (final CSVParser parser =
             CSVFormat.DEFAULT.withHeader().parse(
                 new InputStreamReader(EmployeeSchema.class.getResourceAsStream("employees.csv")))) {
      final Set<String> fields = parser.getHeaderMap().keySet();
      for (final CSVRecord record : parser) {
        Map<String, Object> mapLine = new HashMap<>();
        for (final String fieldName : fields) {
          String s = record.get(fieldName);
          CellDefinition<?> c = employeeCellDefinitionMap.get(fieldName);
          if (c != null && s != null && s.trim().length() > 0) {
            Object val = s;
            if (c.type().equals(Type.LONG)) {
              val = Long.valueOf(s);
            } else if (c.type().equals(Type.BOOL)) {
              val = Boolean.valueOf(s);
            } else if (c.type().equals(Type.INT)) {
              val = Integer.valueOf(s);
            } else if (c.type().equals(Type.DOUBLE)) {
              val = Double.valueOf(s);
            } else if (c.type().equals(Type.BYTES)) {
              String[] sss = s.split(" ");
              byte[] bb = new byte[sss.length];
              int j = 0;
              for (String ss : sss) {
                bb[j++] = Byte.parseByte(ss);
              }
              val = bb;
            }
            mapLine.put(c.name(), val);
          }
        }
        loadedData.add(mapLine);
      }
    }
    return loadedData;
  }
}
