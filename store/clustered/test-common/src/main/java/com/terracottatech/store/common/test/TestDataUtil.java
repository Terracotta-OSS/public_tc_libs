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
package com.terracottatech.store.common.test;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class TestDataUtil {

  private static List<Employee> employeeList;

  static {
    employeeList = new ArrayList<>(50);
    InputStream inputStream = Employee.class.getResourceAsStream("Employee.csv");
    Scanner scanner = new Scanner(inputStream, "UTF-8");
    scanner.nextLine();

    while(scanner.hasNextLine()){
      String line = scanner.nextLine();
      String fields[] = line.split(",");
      int i = 0;
      String name = fields[i++];
      char gender = fields[i++].charAt(0);
      long telephone = Long.parseLong(fields[i++]);
      boolean current = Boolean.parseBoolean(fields[i++]);
      int ssn = Integer.parseInt(fields[i++]);
      double salary = Double.parseDouble(fields[i++]);
      int birthDay = Integer.parseInt(fields[i++]);
      int birthMonth = Integer.parseInt(fields[i++]);
      int birthYear = Integer.parseInt(fields[i++]);
      int houseNumber = Integer.parseInt(fields[i++]);
      String streetAddress = fields[i++];
      String cityAddress = fields[i++];
      String countryAddress = fields[i++];
      double bonus = Double.parseDouble(fields[i++]);
      long cellNUmber = Long.parseLong(fields[i++]);

      Employee employee = new Employee(Employees.generateUniqueEmpID(),
          name,
          gender,
          telephone,
          current,
          ssn,
          salary,
          birthDay,
          birthMonth,
          birthYear,
          houseNumber,
          streetAddress,
          cityAddress,
          countryAddress,
          bonus,
          cellNUmber);

      employeeList.add(employee);
    }

    scanner.close();
  }

  public static List<Employee> getEmployeeList() {
    return employeeList;
  }

}
