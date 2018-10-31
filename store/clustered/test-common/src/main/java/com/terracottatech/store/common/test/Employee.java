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


import com.terracottatech.store.CellSet;
import com.terracottatech.store.Record;
import com.terracottatech.store.definition.BoolCellDefinition;
import com.terracottatech.store.definition.BytesCellDefinition;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.CharCellDefinition;
import com.terracottatech.store.definition.DoubleCellDefinition;
import com.terracottatech.store.definition.IntCellDefinition;
import com.terracottatech.store.definition.LongCellDefinition;
import com.terracottatech.store.definition.StringCellDefinition;

import java.util.Objects;


/**
 * Employee class containing all existing seven cell types used for its attribute definition.
 * This class will be used for various integration testing
 */
public final class Employee implements Comparable<Employee> {

  private Integer empID;
  private String name;
  private Character gender;
  private Long telephone;
  private Boolean current;
  private Integer ssn;
  private Double salary;
  private Integer birthDay;
  private Integer birthMonth;
  private Integer birthYear;
  private Integer houseNumber;
  private String streetAddress;
  private String cityAddress;
  private String countryAddress;
  private Double bonus;
  private Long cellNumber;

  public static final StringCellDefinition NAME = CellDefinition.defineString("name");
  public static final CharCellDefinition GENDER = CellDefinition.defineChar("gender");
  public static final LongCellDefinition TELEPHONE = CellDefinition.defineLong("telephone");
  public static final BoolCellDefinition CURRENT = CellDefinition.defineBool("current");
  public static final IntCellDefinition SSN = CellDefinition.defineInt("ssn");
  public static final DoubleCellDefinition SALARY = CellDefinition.defineDouble("salary");
  public static final BytesCellDefinition SIGNATURE = CellDefinition.defineBytes("signature");

  public static final IntCellDefinition BIRTH_DAY = CellDefinition.defineInt("birthDay");
  public static final IntCellDefinition BIRTH_MONTH = CellDefinition.defineInt("birthMonth");
  public static final IntCellDefinition BIRTH_YEAR = CellDefinition.defineInt("birthYear");

  public static final IntCellDefinition HOUSE_NUMBER_ADDRESS = CellDefinition.defineInt("houseNumber");
  public static final StringCellDefinition  STREET_ADDRESS = CellDefinition.defineString("streetAddress");
  public static final StringCellDefinition  CITY_ADDRESS = CellDefinition.defineString("cityAddress");
  public static final StringCellDefinition  COUNTRY_ADDRESS = CellDefinition.defineString("countryAddress");

  public static final DoubleCellDefinition BONUS = CellDefinition.defineDouble("bonus");

  public static final LongCellDefinition CELL_NUMBER = CellDefinition.defineLong("cellNumber");

  public static final CellDefinition<?> employeeCellDefinitions[] = {NAME,
      GENDER,
      TELEPHONE,
      CURRENT,
      SSN,
      SALARY,
      SIGNATURE,
      BIRTH_DAY,
      BIRTH_MONTH,
      BIRTH_YEAR,
      HOUSE_NUMBER_ADDRESS,
      STREET_ADDRESS,
      CITY_ADDRESS,
      COUNTRY_ADDRESS,
      BONUS,
      CELL_NUMBER};

  private Employee() {}

  public Employee(int empID,
                  String name,
                  char gender,
                  long telephone,
                  boolean current,
                  int ssn,
                  double salary,
                  int birthDay,
                  int birthMonth,
                  int birthYear,
                  int houseNumber,
                  String streetAddress,
                  String cityAddress,
                  String countryAddress,
                  double bonus,
                  long cellNumber) {

    this.empID = empID;
    this.name = name;
    this.gender = gender;
    this.telephone = telephone;
    this.current = current;
    this.ssn = ssn;
    this.salary = salary;
    this.birthDay = birthDay;
    this.birthMonth = birthMonth;
    this.birthYear = birthYear;
    this.houseNumber = houseNumber;
    this.streetAddress = streetAddress;
    this.cityAddress = cityAddress;
    this.countryAddress = countryAddress;
    this.bonus = bonus;
    this.cellNumber = cellNumber;
  }

  public Employee(Record<Integer> record) {
    this.empID = record.getKey();
    this.name = record.get(NAME).orElse(null);
    this.gender = record.get(GENDER).orElse(null);
    this.telephone = record.get(TELEPHONE).orElse(null);
    this.current = record.get(CURRENT).orElse(null);
    this.ssn = record.get(SSN).orElse(null);
    this.salary = record.get(SALARY).orElse(null);
    this.birthDay = record.get(BIRTH_DAY).orElse(null);
    this.birthMonth = record.get(BIRTH_MONTH).orElse(null);
    this.birthYear = record.get(BIRTH_YEAR).orElse(null);
    this.houseNumber = record.get(HOUSE_NUMBER_ADDRESS).orElse(null);
    this.streetAddress = record.get(STREET_ADDRESS).orElse(null);
    this.cityAddress = record.get(CITY_ADDRESS).orElse(null);
    this.countryAddress = record.get(COUNTRY_ADDRESS).orElse(null);
    this.bonus = record.get(BONUS).orElse(null);
    this.cellNumber = record.get(CELL_NUMBER).orElse(null);
  }


  public Integer getEmpID() {
    return empID;
  }

  public String getName() {
    return name;
  }

  public Character getGender() {
    return gender;
  }

  public Long getTelephone() {
    return telephone;
  }

  public Boolean getCurrent() {
    return current;
  }

  public Integer getSsn() {
    return ssn;
  }

  public Double getSalary() {
    return salary;
  }

  public Integer getBirthDay() {
    return birthDay;
  }

  public Integer getBirthMonth() {
    return birthMonth;
  }

  public Integer getBirthYear() {
    return birthYear;
  }

  public Integer getHouseNumber() {
    return houseNumber;
  }

  public String getStreetAddress() {
    return streetAddress;
  }

  public String getCityAddress() {
    return cityAddress;
  }

  public String getCountryAddress() {
    return countryAddress;
  }

  public Double getBonus() {
    return bonus;
  }

  public Long getCellNumber() {
    return cellNumber;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof Employee)) {
      return false;
    }

    Employee employee = (Employee) o;

    return (Objects.equals(empID, employee.empID) &&
        Objects.equals(name, employee.name) &&
        Objects.equals(gender, employee.gender) &&
        Objects.equals(telephone, employee.telephone) &&
        Objects.equals(current, employee.current) &&
        Objects.equals(ssn, employee.ssn) &&
        Objects.equals(salary, employee.salary) &&
        Objects.equals(birthDay, employee.birthDay) &&
        Objects.equals(birthMonth, employee.birthMonth) &&
        Objects.equals(birthYear, employee.birthYear) &&
        Objects.equals(houseNumber, employee.houseNumber) &&
        Objects.equals(streetAddress, employee.streetAddress) &&
        Objects.equals(cityAddress, employee.cityAddress) &&
        Objects.equals(countryAddress, employee.countryAddress) &&
        Objects.equals(bonus, employee.bonus) &&
        Objects.equals(cellNumber, employee.cellNumber));
  }

  @Override
  public int hashCode() {
    return empID;
  }

  @Override
  public String toString() {
    return "Employee{" +
        "empID=" + empID +
        ", name='" + name + '\'' +
        ", gender=" + gender +
        ", telephone=" + telephone +
        ", current=" + current +
        ", ssn=" + ssn +
        ", salary=" + salary +
        ", birthDay=" + birthDay +
        ", birthMonth=" + birthMonth +
        ", birthYear=" + birthYear +
        ", houseNumber=" + houseNumber +
        ", streetAddress='" + streetAddress + '\'' +
        ", cityAddress='" + cityAddress + '\'' +
        ", countryAddress='" + countryAddress + '\'' +
        ", bonus=" + bonus +
        ", cellNumber=" + cellNumber +
        '}';
  }

  @Override
  public int compareTo(Employee o) {
    return empID.compareTo(o.empID);
  }

  public CellSet getCellSet() {
    CellSet  cellSet = new CellSet();

    if (this.name != null) {
      cellSet.add(NAME.newCell(name));
    }

    if (this.gender != null) {
      cellSet.add(GENDER.newCell(gender));
    }

    if (this.telephone != null) {
      cellSet.add(TELEPHONE.newCell(telephone));
    }

    if (this.current != null) {
      cellSet.add(CURRENT.newCell(current));
    }

    if (this.ssn != null) {
      cellSet.add(SSN.newCell(ssn));
    }

    if (this.salary != null) {
      cellSet.add(SALARY.newCell(salary));
    }

    if (this.birthDay != null) {
      cellSet.add(BIRTH_DAY.newCell(birthDay));
    }

    if (this.birthMonth != null) {
      cellSet.add(BIRTH_MONTH.newCell(birthMonth));
    }

    if (this.birthYear != null) {
      cellSet.add(BIRTH_YEAR.newCell(birthYear));
    }

    if (this.houseNumber != null) {
      cellSet.add(HOUSE_NUMBER_ADDRESS.newCell(houseNumber));
    }

    if (this.streetAddress != null) {
      cellSet.add(STREET_ADDRESS.newCell(streetAddress));
    }

    if (this.cityAddress != null) {
      cellSet.add(CITY_ADDRESS.newCell(cityAddress));
    }

    if (this.countryAddress != null) {
      cellSet.add(COUNTRY_ADDRESS.newCell(countryAddress));
    }

    if (this.bonus != null) {
      cellSet.add(BONUS.newCell(bonus));
    }

    if (this.cellNumber != null) {
      cellSet.add(CELL_NUMBER.newCell(cellNumber));
    }

    return cellSet;
  }

}
