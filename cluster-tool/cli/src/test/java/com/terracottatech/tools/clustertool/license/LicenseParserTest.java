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

import com.terracottatech.License;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class LicenseParserTest {
  private static final String EHCACHE_LICENSE_FILE_NAME = "TerracottaEhcache101Linux.xml";
  private static final String TCDB_LICENSE_FILE_NAME = "TerracottaDB101Linux.xml";

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();
  private final LicenseParser licenseParser = new DefaultLicenseParser();

  @Test
  public void testTCDBLicenseParser() {
    try {
      License license = licenseParser.parse(getTCDBLicensePath());
      assertThat(license.getCapabilityLimitMap(), is(getTCDBLicense().getCapabilityLimitMap()));
      assertThat(license.getExpiryDate(), is(getTCDBLicense().getExpiryDate()));
    } catch (Exception e) {
      fail("Not expecting Exception: " + e);
    }
  }

  @Test
  public void testEhcacheLicenseParser() {
    try {
      License license = licenseParser.parse(getEhcacheLicensePath());
      assertThat(license.getCapabilityLimitMap(), is(getEhCacheLicense().getCapabilityLimitMap()));
      assertThat(license.getExpiryDate(), is(getEhCacheLicense().getExpiryDate()));
    } catch (Exception e) {
      fail("Not expecting Exception: " + e);
    }
  }

  private License getEhCacheLicense() {
    Map<String, Long> capabilityLimitMap = new HashMap<>();
    capabilityLimitMap.put("SearchCompute", 0L);
    capabilityLimitMap.put("Caching", 1L);
    capabilityLimitMap.put("TCStore", 0L);
    capabilityLimitMap.put("TMC", 1L);
    capabilityLimitMap.put("MultiStripe", 1L);
    capabilityLimitMap.put("Security", 1L);
    capabilityLimitMap.put(DefaultLicenseParser.CAPABILITY_OFFHEAP, 1024000L);

    LocalDate expiryDate = LocalDate.parse("2050/01/01", DefaultLicenseParser.EXPIRY_DATE_TIME_FORMATTER);
    return new License(capabilityLimitMap, expiryDate);
  }

  private License getTCDBLicense() {
    Map<String, Long> capabilityLimitMap = new HashMap<>();
    capabilityLimitMap.put("SearchCompute", 1L);
    capabilityLimitMap.put("Caching", 1L);
    capabilityLimitMap.put("TCStore", 1L);
    capabilityLimitMap.put("TMC", 1L);
    capabilityLimitMap.put("MultiStripe", 1L);
    capabilityLimitMap.put("Security", 1L);
    capabilityLimitMap.put(DefaultLicenseParser.CAPABILITY_OFFHEAP, 1024000L);

    LocalDate expiryDate = LocalDate.parse("2020/01/01", DefaultLicenseParser.EXPIRY_DATE_TIME_FORMATTER);
    return new License(capabilityLimitMap, expiryDate);
  }

  private String getTCDBLicensePath() {
    try {
      return Paths.get(LicenseParserTest.class.getResource("/" + TCDB_LICENSE_FILE_NAME).toURI()).toString();
    } catch (URISyntaxException e) {
      throw new AssertionError("License file " + TCDB_LICENSE_FILE_NAME + " URL isn't formatted correctly");
    }
  }

  private String getEhcacheLicensePath() {
    try {
      return Paths.get(LicenseParserTest.class.getResource("/" + EHCACHE_LICENSE_FILE_NAME).toURI()).toString();
    } catch (URISyntaxException e) {
      throw new AssertionError("License file " + EHCACHE_LICENSE_FILE_NAME + " URL isn't formatted correctly");
    }
  }
}
