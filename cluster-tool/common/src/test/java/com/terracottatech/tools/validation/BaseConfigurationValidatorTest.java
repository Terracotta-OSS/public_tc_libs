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
package com.terracottatech.tools.validation;

import com.terracottatech.tools.config.ConfigurationParser;
import com.terracottatech.tools.config.DefaultConfigurationParser;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;

public class BaseConfigurationValidatorTest {
  static final String CONFIG_TYPE_MISMATCH = "Mismatched config types";
  static final String OFFHEAP_MISMATCH = "Mismatched off-heap";
  static final String DATA_DIRECTORIES_MISMATCH = "Mismatched data directories";
  static final String FAILOVER_PRIORITY_MISMATCH = "Mismatched failover priority";
  static final String SECURITY_CONFIGURATION_MISMATCH = "Mismatched security configuration";

  ConfigurationParser configurationParser;
  ConfigurationValidator validator;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Before
  public void init() {
    this.configurationParser = new DefaultConfigurationParser();
    this.validator = new ConfigurationValidator();
  }

  String readContent(String resourceConfigName) {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      InputStream inputStream = ConfigurationValidator_ValidateTest.class.getResourceAsStream("/" + resourceConfigName);
      byte[] buffer = new byte[1024];
      int length;
      while ((length = inputStream.read(buffer)) != -1) {
        baos.write(buffer, 0, length);
      }
      return baos.toString("UTF-8");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
