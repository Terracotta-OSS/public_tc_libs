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
package com.terracottatech.tools.clustertool.license;


import com.softwareag.common.lic.SagLic;
import com.softwareag.common.lic.SagLicException;
import com.terracottatech.License;
import com.terracottatech.LicenseException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;


public class DefaultLicenseParser implements LicenseParser {

  public static final String CAPABILITY_OFFHEAP = "OffHeap";

  public static final DateTimeFormatter EXPIRY_DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy/MM/dd", Locale.ENGLISH);

  private static final String COMPONENT_PRODUCT_INFO = "ProductInfo";
  private static final String COMPONENT_LICENSE_INFO = "LicenseInfo";
  private static final String COMPONENT_TERRACOTTA = "Terracotta";

  private static final String FIELD_EXPIRY_DATE = "ExpirationDate";

  private static final String FIELD_PRICE_UNIT = "PriceUnit";
  private static final String FIELD_PRICE_QUANTITY = "PriceQuantity";

  private static final String FIELD_CACHING = "Caching";
  private static final String FIELD_TMC = "TMC";
  private static final String FIELD_MULTI_STRIPE = "MultiStripe";
  private static final String FIELD_SECURITY = "Security";
  private static final String FIELD_TCSTORE = "TCStore";
  private static final String FIELD_SEARCH_COMPUTE = "SearchCompute";

  private final SagLic sagLic;

  public DefaultLicenseParser() {
    this.sagLic = new SagLic();
  }

  @Override
  public License parse(String licenseFilePath) throws LicenseException {
    ensureValidLicense(licenseFilePath);
    return parseLicense(licenseFilePath);
  }


  /**
   * Parse License information from the provided license File Path.
   * @param licenseFilePath
   * @return
   * @throws Exception
   */
  private License parseLicense(String licenseFilePath) throws LicenseException {
    LocalDate expiryDate = LocalDate.parse(readLicenseField(licenseFilePath, COMPONENT_PRODUCT_INFO, FIELD_EXPIRY_DATE), EXPIRY_DATE_TIME_FORMATTER);

    Map<String, Long> capabilityLimitMap = new HashMap<>();
    capabilityLimitMap.put(FIELD_CACHING, readLicenseCapability(licenseFilePath, COMPONENT_TERRACOTTA, FIELD_CACHING));
    capabilityLimitMap.put(FIELD_TMC, readLicenseCapability(licenseFilePath, COMPONENT_TERRACOTTA, FIELD_TMC));
    capabilityLimitMap.put(FIELD_MULTI_STRIPE, readLicenseCapability(licenseFilePath, COMPONENT_TERRACOTTA, FIELD_MULTI_STRIPE));
    capabilityLimitMap.put(FIELD_SECURITY, readLicenseCapability(licenseFilePath, COMPONENT_TERRACOTTA, FIELD_SECURITY));
    capabilityLimitMap.put(FIELD_TCSTORE, readLicenseCapability(licenseFilePath, COMPONENT_TERRACOTTA, FIELD_TCSTORE));
    capabilityLimitMap.put(FIELD_SEARCH_COMPUTE, readLicenseCapability(licenseFilePath, COMPONENT_TERRACOTTA, FIELD_SEARCH_COMPUTE));
    capabilityLimitMap.put(CAPABILITY_OFFHEAP, readOffHeapLimit(licenseFilePath));

    return new License(capabilityLimitMap, expiryDate);
  }

  private long readLicenseCapability(String licenseFilePath, String component, String field) throws LicenseException {
    String value = readLicenseField(licenseFilePath, component, field);
    if (value.startsWith("y")) {
      return 1;
    }

    if (value.startsWith("n")) {
      return 0;
    }

    throw new IllegalStateException(String.format("Unknown Field: %s:%s", component, field));
  }

  private long readOffHeapLimit(String licenseFilePath) throws LicenseException {
    String priceUnitField = readLicenseField(licenseFilePath, COMPONENT_LICENSE_INFO, FIELD_PRICE_UNIT);
    long priceQuantityField = Long.parseLong(readLicenseField(licenseFilePath, COMPONENT_LICENSE_INFO, FIELD_PRICE_QUANTITY));

    switch (priceUnitField) {
      case "GBS":
        return priceQuantityField * 1024;
    }

    throw new IllegalStateException("Invalid PriceUnit field: " + priceUnitField);
  }

  /**
   * Extract the information of the given field from the provided component.
   * @param licenseFilePath License File Path.
   * @param component Component to read from the license
   * @param field Field to be read from the component
   * @return Component field value.
   * @throws Exception
   */
  private String readLicenseField(String licenseFilePath, String component, String field) throws LicenseException {
    try {
      return sagLic.LICreadParameter(licenseFilePath, component, field);
    } catch (SagLicException ex) {
      throw new LicenseException(LicenseException.Type.INVALID_LICENSE, String.format("Unable to read field '%s:%s'. Error: %s", component, field, ex));
    }
  }

  /**
   * Ensure the license is valid before extracting the required License Information.
   * @param licenseFilePath
   * @throws Exception
   */
  private void ensureValidLicense(String licenseFilePath) throws LicenseException {
    ensureValidHashcode(licenseFilePath);
    ensureValidExpiryDate(licenseFilePath);
  }

  /**
   * Ensure the license hash code is valid.
   *
   * @param licenseFilePath License File Path.
   * @throws Exception
   */
  private void ensureValidHashcode(String licenseFilePath) throws LicenseException {
    try {
      String licenseStr = new String(Files.readAllBytes(Paths.get(licenseFilePath)), StandardCharsets.US_ASCII);
      sagLic.LICcheckSignature(licenseStr);
    } catch (SagLicException ex) {
      throw new LicenseException(LicenseException.Type.INVALID_LICENSE, "Invalid license");
    } catch (IOException e) {
      throw new LicenseException(LicenseException.Type.INVALID_LICENSE, "Unable to read license file: " + e.getMessage());
    }
  }

  /**
   * Ensure license is not yet expired.
   *
   * @param licenseFilePath
   * @throws Exception
   */
  private void ensureValidExpiryDate(String licenseFilePath) throws LicenseException {
    try {
      sagLic.LICcheckExpiration(licenseFilePath, COMPONENT_PRODUCT_INFO);
    } catch (SagLicException ex) {
      throw new LicenseException(LicenseException.Type.INVALID_LICENSE, "Provided license has expired");
    }
  }
}
