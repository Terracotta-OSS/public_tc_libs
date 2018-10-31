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
package com.terracottatech.tools.tests;


import com.terracottatech.testing.rules.EnterpriseCluster;
import org.junit.ClassRule;

import static com.terracottatech.testing.rules.EnterpriseExternalClusterBuilder.newCluster;
import static com.terracottatech.tools.tests.BackupTestHelper.resourceConfigWithBackupPath;


public class BackupMultiStripeIT extends AbstractBackupIT {

  @ClassRule
  public static final EnterpriseCluster CLUSTER = newCluster(1, 1).withPlugins(resourceConfigWithBackupPath(backupFolder.getRoot().getAbsolutePath())).build(true);

  @Override
  protected EnterpriseCluster getCluster() {
    return CLUSTER;
  }
}