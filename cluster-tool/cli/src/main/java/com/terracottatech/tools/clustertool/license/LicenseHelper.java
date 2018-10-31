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


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class LicenseHelper {

  private static final String CLUSTER_TOOL_DIR_PROP_NAME = "clustertool.dir";
  private static final String LICENSE_FILE_NAME = "license.xml";

  private static final Logger LOGGER = LoggerFactory.getLogger(LicenseHelper.class);

  public static String getResolvedLicenseFilePath(String licenseFilePath) {
    if (licenseFilePath == null || licenseFilePath.isEmpty()) {
      return defaultLicenseFilePath();
    }

    return licenseFilePath;
  }

  private static String defaultLicenseFilePath() {
    String clusterToolDir = System.getProperty(CLUSTER_TOOL_DIR_PROP_NAME);

    LOGGER.debug("clustertool.dir: {}", clusterToolDir);
    // Return empty license file path if clustertool.dir system property is not set.
    // This shouldn't happen because the property is set by cluster-tool.(bat|sh) script.
    if (clusterToolDir == null) {
      return "";
    }

    Path licenseFilePath = Paths.get(clusterToolDir, "conf", LICENSE_FILE_NAME);
    if (!Files.isRegularFile(licenseFilePath)) {
      LOGGER.debug("'" + LICENSE_FILE_NAME + "' is not present in cluster tool conf directory.");
      return "";
    }

    return licenseFilePath.toString();
  }
}
