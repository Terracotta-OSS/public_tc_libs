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
package com.terracottatech.testing.master;

import com.terracottatech.connection.EnterpriseConnectionPropertyNames;
import com.terracottatech.licensing.client.LicenseInstaller;
import com.terracottatech.licensing.common.LicenseCreator;
import com.terracottatech.testing.lock.GlobalFilePortLocker;
import com.terracottatech.testing.lock.LocalPortLocker;
import com.terracottatech.testing.lock.LockingPortChooser;
import com.terracottatech.testing.lock.LockingPortChoosers;
import com.terracottatech.testing.lock.MuxPortLock;
import com.terracottatech.testing.lock.MuxPortLocker;
import com.terracottatech.testing.lock.RandomPortAllocator;
import com.terracottatech.testing.lock.SocketPortLocker;
import com.terracottatech.tools.clustertool.managers.TopologyManager;

import com.terracottatech.testing.api.ExtendedTestClusterConfiguration;

import org.terracotta.connection.ConnectionPropertyNames;
import org.terracotta.testing.common.Assert;
import org.terracotta.testing.logging.ContextualLogger;
import org.terracotta.testing.logging.VerboseManager;
import org.terracotta.testing.master.AbstractHarnessEntry;
import org.terracotta.testing.master.BasicClientArgumentBuilder;
import org.terracotta.testing.master.ClusterInfo;
import org.terracotta.testing.master.CommonIdioms;
import org.terracotta.testing.master.DebugOptions;
import org.terracotta.testing.master.GalvanFailureException;
import org.terracotta.testing.master.GalvanStateInterlock;
import org.terracotta.testing.master.IMultiProcessControl;
import org.terracotta.testing.master.ReadyStripe;
import org.terracotta.testing.master.ServerInfo;
import org.terracotta.testing.master.TestStateManager;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.terracottatech.utilities.InetSocketAddressConvertor.getInetSocketAddresses;
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;


public class ExtendedHarnessEntry extends AbstractHarnessEntry<ExtendedTestClusterConfiguration> {

  private final Map<String, ServerInfo> allServers = new HashMap<>();
  private final TestStateManager stateManager = new TestStateManager();
  private final LockingPortChooser portChooser = LockingPortChoosers.getFileLockingPortChooser();

  private List<ReadyStripe> stripes;
  private IMultiProcessControl processControl;
  private GalvanStateInterlock[] subInterlocks;
  private MultiStripeInterlock interlock;
  private MuxPortLock portLocks;

  // Run the one configuration.
  @Override
  protected void runOneConfiguration(VerboseManager verboseManager, DebugOptions debugOptions, CommonHarnessOptions harnessOptions, ExtendedTestClusterConfiguration runConfiguration) throws IOException, GalvanFailureException {
    int stripesToCreate = runConfiguration.stripeCount;
    int serversToCreate = runConfiguration.serversPerStripeCount;
    runOneExtendedConfiguration(verboseManager, debugOptions, harnessOptions, stripesToCreate, serversToCreate, runConfiguration.getName());
  }

  public void startFromRule(VerboseManager verboseManager, String kitOriginPath, String configTestDirectory, List<String> extraJarPaths,
                            String namespaceFragment, String serviceFragment, Path securityRootDirectory,
                            Properties tcProperties, Properties serverProperties, int serverDebugPortStart, int[] clusterSize,
                            boolean configureCluster, String clusterName, String logConfig, int clientReconnectWindowTime,
                            int failoverPriorityVoterCount) throws IOException, GalvanFailureException {

    CommonHarnessOptions commonHarnessOptions = new CommonHarnessOptions();
    commonHarnessOptions.configTestDirectory = configTestDirectory;
    commonHarnessOptions.kitOriginPath = kitOriginPath;
    commonHarnessOptions.extraJarPaths = extraJarPaths;
    commonHarnessOptions.namespaceFragment = namespaceFragment;
    commonHarnessOptions.serviceFragment = serviceFragment;
    commonHarnessOptions.tcProperties = tcProperties;
    commonHarnessOptions.serverProperties = serverProperties;
    commonHarnessOptions.clientReconnectWindowTime = clientReconnectWindowTime;
    commonHarnessOptions.failoverPriorityVoterCount = failoverPriorityVoterCount;

    runEnterpriseCluster(verboseManager, commonHarnessOptions, serverDebugPortStart, clusterSize, configureCluster, clusterName, logConfig, securityRootDirectory);
    interlock = new MultiStripeInterlock(subInterlocks, null);
  }

  private void runEnterpriseCluster(VerboseManager verboseManager, CommonHarnessOptions harnessOptions, int serverDebugPortStart,
                                    int[] clusterSize, boolean configureCluster, String clusterName, String logConfig,
                                    Path securityRootDirectory) throws IOException, GalvanFailureException {
    Assert.assertTrue(clusterSize.length > 0);
    for (int i = 0; i < clusterSize.length; i++) {
      Assert.assertTrue(clusterSize[i] > 0);
    }

    ReadyStripe[] stripes = new ReadyStripe[clusterSize.length];
    subInterlocks = new GalvanStateInterlock[clusterSize.length];
    for (int i = 0; i < clusterSize.length; ++i) {
      String stripeName = "stripe" + i;
      // We will split the start number by 100 so it is more clear which servers are in which stripes.
      int serverStartNumber = (i * 100);
      // Determine how many servers there are per stripe and use that to spread the debug ports (if set).
      int stripeDebugPortBase = (serverDebugPortStart > 0)
          ? (serverDebugPortStart + (i * clusterSize[i]))
          : 0;

      // Calculate the size of the port range we need:  each server needs 2 ports.
      int portsRequired = clusterSize[i] * 2;

      CommonIdioms.StripeConfiguration stripeConfiguration = new CommonIdioms.StripeConfiguration();
      stripeConfiguration.kitOriginPath = harnessOptions.kitOriginPath;
      stripeConfiguration.testParentDirectory = harnessOptions.configTestDirectory;
      stripeConfiguration.serversToCreate = clusterSize[i];
      stripeConfiguration.serverHeapInM = harnessOptions.serverHeapInM;
      stripeConfiguration.serverStartPort = chooseRandomPortRange(portsRequired);
      stripeConfiguration.serverDebugPortStart = stripeDebugPortBase;
      stripeConfiguration.serverStartNumber = serverStartNumber;
      stripeConfiguration.extraJarPaths = harnessOptions.extraJarPaths;
      stripeConfiguration.namespaceFragment = harnessOptions.namespaceFragment;
      stripeConfiguration.serviceFragment = harnessOptions.serviceFragment;
      stripeConfiguration.tcProperties = harnessOptions.tcProperties;
      stripeConfiguration.serverProperties = harnessOptions.serverProperties;
      if (harnessOptions.clientReconnectWindowTime > 0) {
        // TODO this should be defaulted in org.terracotta.testing.master.AbstractHarnessEntry.CommonHarnessOptions
        stripeConfiguration.clientReconnectWindowTime = harnessOptions.clientReconnectWindowTime;
      }
      stripeConfiguration.failoverPriorityVoterCount = harnessOptions.failoverPriorityVoterCount;
      stripeConfiguration.stripeName = stripeName;
      stripeConfiguration.logConfigExtension = logConfig;
      GalvanStateInterlock interlock = new GalvanStateInterlock(verboseManager.createComponentManager("[Interlock" + i + "]").createHarnessLogger(), stateManager);
      stripes[i] = CommonIdioms.setupConfigureAndStartStripe(interlock, stateManager, verboseManager, stripeConfiguration);
      subInterlocks[i] = interlock;
    }

    // At this point, all the stripes are running so we can create the cluster tool entity and upload the cluster config.
    ContextualLogger clusterToolLogger = verboseManager.createComponentManager("[ClusterTool]").createHarnessLogger();
    if (configureCluster) {
      // Run cluster tool to upload the combined config.
      ArrayList<String> configs = new ArrayList<>();
      for (ReadyStripe stripe : stripes) {
        configs.add(stripe.configText);
      }
      if(clusterName == null) {
        clusterName = "primary";
      }
      clusterToolLogger.output("Executing cluster tool config with arguments 'clusterName': " + clusterName
                               + " and 'configs': " + configs);
      TopologyManager topologyManager = new TopologyManager();
      LicenseInstaller licenseInstaller = new LicenseInstaller();

      String securityRootAsString = (securityRootDirectory == null) ? null : securityRootDirectory.toString();
      topologyManager.setSecurityRootDirectory(securityRootAsString);
      topologyManager.configureByConfigs(clusterName, configs);

      Properties properties = new Properties();
      if (securityRootAsString != null) {
        properties.setProperty(EnterpriseConnectionPropertyNames.SECURITY_ROOT_DIRECTORY, securityRootAsString);
      }
      URI stripeUri = URI.create(stripes[0].stripeUri);
      properties.setProperty(ConnectionPropertyNames.CONNECTION_TYPE, stripeUri.getScheme());
      String[] servers = stripeUri.getAuthority().split(",");
      licenseInstaller.installLicense(getInetSocketAddresses(servers), LicenseCreator.createTestLicense(), properties);
    } else {
      clusterToolLogger.output("Leaving cluster not configured");
    }
    clusterToolLogger.output("Cluster URI: " + stripes[0].stripeUri);

    // We now need to walk these stripes to create the control.
    IMultiProcessControl[] subControl = new IMultiProcessControl[clusterSize.length];
    for (int i = 0; i < clusterSize.length; ++i) {
      subControl[i] = stripes[i].stripeControl;
      for (ServerInfo serverInfo : stripes[i].clusterInfo.getServersInfo()) {
        allServers.put(serverInfo.getName(), serverInfo);
      }
    }
    this.stripes = unmodifiableList(asList(stripes));
    processControl = new MultiStripeProcessControl(subControl);

  }

  // An active-active cluster.
  private void runOneExtendedConfiguration(VerboseManager verboseManager, DebugOptions debugOptions, CommonHarnessOptions harnessOptions, int stripesToCreate, int serversPerStripe, String clusterName) throws IOException, GalvanFailureException {

    int[] clusterSize = new int[stripesToCreate];
    for (int i = 0; i < clusterSize.length; i++) {
      clusterSize[i] = serversPerStripe;
    }

    runEnterpriseCluster(verboseManager, harnessOptions, debugOptions.serverDebugPortStart, clusterSize, true, clusterName, null, null);
    // The cluster is now running so install and run the clients.
    CommonIdioms.ClientsConfiguration clientsConfiguration = new CommonIdioms.ClientsConfiguration();
    clientsConfiguration.testParentDirectory = harnessOptions.configTestDirectory;
    clientsConfiguration.clientClassPath = harnessOptions.clientClassPath;
    clientsConfiguration.clientsToCreate = harnessOptions.clientsToCreate;
    clientsConfiguration.clientArgumentBuilder = new BasicClientArgumentBuilder(harnessOptions.testClassName, harnessOptions.errorClassName);
    clientsConfiguration.connectUri = stripes.get(0).stripeUri;
    clientsConfiguration.clusterInfo = new ClusterInfo(allServers);
    clientsConfiguration.numberOfStripes = clusterSize.length;
    clientsConfiguration.numberOfServersPerStripe = serversPerStripe;
    clientsConfiguration.setupClientDebugPort = debugOptions.setupClientDebugPort;
    clientsConfiguration.destroyClientDebugPort = debugOptions.destroyClientDebugPort;
    clientsConfiguration.testClientDebugPortStart = debugOptions.testClientDebugPortStart;
    GalvanStateInterlock clientInterlock = new GalvanStateInterlock(verboseManager.createComponentManager("[ClientInterlock]").createHarnessLogger(), stateManager);
    interlock = new MultiStripeInterlock(subInterlocks, clientInterlock);
    CommonIdioms.installAndRunClients(interlock, stateManager, verboseManager, clientsConfiguration, processControl);
    // NOTE:  waitForFinish() throws GalvanFailureException on failure.
    try {
      stateManager.waitForFinish();
    } finally {
      // No matter what happened, shut down the test.
      interlock.forceShutdown();
    }
  }

  public TestStateManager getStateManager() {
    return stateManager;
  }

  public List<ReadyStripe> getStripes() {
    return stripes;
  }

  public IMultiProcessControl getProcessControl() {
    return processControl;
  }

  public MultiStripeInterlock getInterlock() {
    return interlock;
  }

  public MuxPortLock getPortLocks() {
    return portLocks;
  }

  @Override
  public int chooseRandomPortRange(int portCount) {
    MuxPortLock multiPortLock = portChooser.choosePorts(portCount);

    synchronized (this) {
      if (portLocks == null) {
        portLocks = multiPortLock;
      } else {
        portLocks = portLocks.combine(multiPortLock);
      }
    }

    return multiPortLock.getPort();
  }
}
