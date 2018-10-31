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
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.SystemOutRule;
import org.junit.rules.TemporaryFolder;
import org.terracotta.connection.Connection;
import org.terracotta.connection.ConnectionFactory;
import org.terracotta.connection.ConnectionPropertyNames;
import org.terracotta.management.entity.nms.NmsConfig;
import org.terracotta.management.entity.nms.client.DefaultNmsService;
import org.terracotta.management.entity.nms.client.NmsEntity;
import org.terracotta.management.entity.nms.client.NmsEntityFactory;
import org.terracotta.management.model.message.Message;
import org.terracotta.management.model.notification.ContextualNotification;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.terracottatech.testing.rules.EnterpriseExternalClusterBuilder.newCluster;
import static com.terracottatech.tools.tests.BackupTestHelper.backupViaClusterTool;
import static com.terracottatech.tools.tests.BackupTestHelper.populateEhCache;
import static com.terracottatech.tools.tests.BackupTestHelper.populateTCStore;
import static com.terracottatech.tools.tests.BackupTestHelper.resourceConfigWithBackupPath;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsMapContaining.hasKey;

public class BackupNotificationIT {
  @ClassRule
  public static TemporaryFolder backupFolder = new TemporaryFolder();

  static {
    try {
      backupFolder.create();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  @ClassRule
  public static final EnterpriseCluster CLUSTER = newCluster(2).withPlugins(resourceConfigWithBackupPath(backupFolder.getRoot().getAbsolutePath())).build(true);

  @Rule
  public final SystemOutRule systemOutRule = new SystemOutRule().enableLog();

  private DefaultNmsService nmsService;
  private Connection managementConnection;

  @Before
  public void setup() throws Exception {
    CLUSTER.getClusterControl().waitForActive();
    CLUSTER.getClusterControl().waitForRunningPassivesInStandby();
    connectManagementClient(0);
  }

  @After
  public void cleanup() throws IOException {
    if (managementConnection != null) {
      managementConnection.close();
    }
    backupFolder.delete();
  }

  @Test
  public void testBackupNotifications() throws Exception {
    populateEhCache(CLUSTER);
    populateTCStore(CLUSTER);
    backupViaClusterTool(CLUSTER);

    List<Message> messages = nmsService.waitForMessage(message ->
        message.getType().equals("NOTIFICATION")
            && message.unwrap(ContextualNotification.class).stream().anyMatch(notification -> notification.getType().equals("BACKUP_COMPLETED")));

    List<ContextualNotification> backupNotifications = messages.stream()
        .filter(message -> message.getType().equals("NOTIFICATION"))
        .flatMap(message -> message.unwrap(ContextualNotification.class).stream())
        .filter(notification -> notification.getType().equals("BACKUP_STARTED") || notification.getType().equals("BACKUP_COMPLETED"))
        .collect(Collectors.toList());

    // only from active server
    assertThat(backupNotifications.size(), is(2));

    assertThat(backupNotifications.get(0).getAttributes().size(), equalTo(2));
    assertThat(backupNotifications.get(0).getAttributes(), hasKey("backupStartTime"));
    assertThat(backupNotifications.get(0).getAttributes(), hasKey("backupLocation"));

    assertThat(backupNotifications.get(1).getAttributes().size(), equalTo(5));
    assertThat(backupNotifications.get(1).getAttributes(), hasKey("backupStartTime"));
    assertThat(backupNotifications.get(1).getAttributes(), hasKey("backupEndTime"));
    assertThat(backupNotifications.get(1).getAttributes(), hasKey("backupDuration"));
    assertThat(backupNotifications.get(1).getAttributes(), hasKey("backupResult"));
    assertThat(backupNotifications.get(1).getAttributes(), hasKey("backupLocation"));
  }

  private void connectManagementClient(int stripeIdx) throws Exception {
    Properties properties = new Properties();
    properties.setProperty(ConnectionPropertyNames.CONNECTION_NAME, getClass().getSimpleName());
    properties.setProperty(ConnectionPropertyNames.CONNECTION_TIMEOUT, "5000");
    this.managementConnection = ConnectionFactory.connect(CLUSTER.getStripeConnectionURI(stripeIdx), properties);
    NmsEntityFactory nmsEntityFactory = new NmsEntityFactory(managementConnection, getClass().getSimpleName());
    NmsEntity nmsEntity = nmsEntityFactory.retrieveOrCreate(new NmsConfig()
        .setStripeName("stripe-" + stripeIdx));
    this.nmsService = new DefaultNmsService(nmsEntity);
    this.nmsService.setOperationTimeout(60, TimeUnit.SECONDS);
  }
}