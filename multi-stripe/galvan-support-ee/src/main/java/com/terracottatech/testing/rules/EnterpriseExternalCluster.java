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
package com.terracottatech.testing.rules;

import com.tc.util.Assert;
import com.terracottatech.testing.lock.MuxPortLock;
import com.terracottatech.testing.master.ExtendedHarnessEntry;
import com.terracottatech.testing.master.MultiStripeInterlock;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.terracotta.connection.Connection;
import org.terracotta.connection.ConnectionException;
import org.terracotta.connection.ConnectionFactory;
import org.terracotta.passthrough.IClusterControl;
import org.terracotta.testing.logging.VerboseLogger;
import org.terracotta.testing.logging.VerboseManager;
import org.terracotta.testing.master.GalvanFailureException;
import org.terracotta.testing.master.IMultiProcessControl;
import org.terracotta.testing.master.ReadyStripe;
import org.terracotta.testing.master.TestStateManager;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.lang.System.arraycopy;

public class EnterpriseExternalCluster extends EnterpriseCluster {

  private static boolean IS_WIN = System.getProperty("os.name", "unknown").toLowerCase().contains("win");

  private final File clusterDirectory;
  private final int[] clusterSize;
  private final List<File> serverJars;
  private final String namespaceFragment;
  private final String pluginsFragment;
  private final Properties tcProperties;
  private final Supplier<Properties> serverPropertiesSupplier;
  private final Path securityRootDirectory;
  private final boolean configureCluster;
  private final String clusterName;
  private final String logConfig;
  private final int clientReconnectWindowTime;
  private final int failoverPriorityVoterCount;
  private final VerboseLogger harnessLogger;

  private String displayName;
  private List<ReadyStripe> clusterStripes;
  private MultiStripeInterlock interlock;
  private MuxPortLock portLocks;
  private TestStateManager stateManager;
  private IMultiProcessControl clusterControl;
  // Note that the clientThread is actually the main thread of the JUnit runner.
  private final Thread clientThread;
  // We keep a flag to describe whether or not we are currently trying to interrupt the clientThread during what is
  // probably its join on shepherdingThread (as that can be ignored).
  private volatile boolean isInterruptingClient;
  private Thread shepherdingThread;
  private boolean isSafe;
  private File testParentDirectory;

  protected EnterpriseExternalCluster(File clusterDirectory, int[] clusterSize, List<File> serverJars,
                                      String namespaceFragment, String pluginsFragment,
                                      Properties tcProperties, Supplier<Properties> serverPropertiesSupplier,
                                      Path securityRootDirectory, boolean configureCluster, String clusterName,
                                      String logConfig, int clientReconnectWindowTime, int failoverPriorityVoterCount) {
    this.configureCluster = configureCluster;
    this.clusterName = clusterName;
    this.logConfig = logConfig;
    this.clientReconnectWindowTime = clientReconnectWindowTime;
    this.failoverPriorityVoterCount = failoverPriorityVoterCount;
    if (clusterDirectory == null) {
      throw new NullPointerException("Cluster directory must be non-null");
    }
    if (clusterSize.length < 1) {
      throw new IllegalArgumentException("Must be at least one server in the cluster");
    }
    for (int i : clusterSize) {
      if (i < 1) {
        throw new IllegalArgumentException("Server count in stripe must be a positive integer greater than 0");
      }
    }
    if (serverJars == null) {
      throw new NullPointerException("Server JARs list must be non-null");
    }
    if (namespaceFragment == null) {
      throw new NullPointerException("Namespace fragment must be non-null");
    }
    if (pluginsFragment == null) {
      throw new NullPointerException("Service fragment must be non-null");
    }
    if (tcProperties == null) {
      throw new NullPointerException("tcProperties must be non-null");
    }
    if (serverPropertiesSupplier == null) {
      throw new NullPointerException("serverPropertiesSupplier must be non-null");
    }

    if (clusterDirectory.exists()) {
      if (clusterDirectory.isFile()) {
        throw new IllegalArgumentException("Cluster directory is a file: " + clusterDirectory);
      }
    } else {
      boolean didCreateDirectories = clusterDirectory.mkdirs();
      if (!didCreateDirectories) {
        throw new IllegalArgumentException("Cluster directory could not be created: " + clusterDirectory);
      }
    }
    this.clusterDirectory = clusterDirectory;
    this.clusterSize = new int[clusterSize.length];
    arraycopy(clusterSize, 0, this.clusterSize, 0, clusterSize.length);
    this.namespaceFragment = namespaceFragment;
    this.pluginsFragment = pluginsFragment;
    this.tcProperties = tcProperties;
    this.serverPropertiesSupplier = serverPropertiesSupplier;
    this.securityRootDirectory = securityRootDirectory;
    this.serverJars = serverJars;
    harnessLogger = new VerboseLogger(System.out, null);
    this.clientThread = Thread.currentThread();
  }

  @Override
  public Statement apply(Statement base, Description description) {
    String methodName = description.getMethodName();
    Class<?> testClass = description.getTestClass();
    if (methodName == null) {
      if (testClass == null) {
        this.displayName = description.getDisplayName();
      } else {
        this.displayName = testClass.getSimpleName();
      }
    } else if (testClass == null) {
      this.displayName = description.getDisplayName();
    } else {
      this.displayName = testClass.getSimpleName() + "#" + methodName;
    }
    return super.apply(base, description);
  }

  public void manualStart(String displayName) throws Throwable {
    this.displayName = displayName;
    internalStart();
  }

  @Override
  protected void before() throws Throwable {
    internalStart();
  }

  private void internalStart() throws Throwable {
    VerboseLogger fileHelpersLogger = new VerboseLogger(null, null);
    VerboseLogger clientLogger = null;
    VerboseLogger serverLogger = new VerboseLogger(System.out, System.err);
    VerboseManager verboseManager = new VerboseManager("", harnessLogger, fileHelpersLogger, clientLogger, serverLogger);
    VerboseManager displayVerboseManager = verboseManager.createComponentManager("[" + displayName + "]");

    String kitInstallationPath = System.getProperty("kitInstallationPath");
    harnessLogger.output("Using kitInstallationPath: \"" + kitInstallationPath + "\"");

    testParentDirectory = createTempFolder();

    List<String> serverJarPaths = convertToStringPaths(serverJars);
    String debugPortString = System.getProperty("serverDebugStartPort");
    int serverDebugStartPort = (null != debugPortString)
        ? Integer.parseInt(debugPortString)
        : 0;

    ExtendedHarnessEntry extendedHarnessEntry = new ExtendedHarnessEntry();
    extendedHarnessEntry.startFromRule(displayVerboseManager, kitInstallationPath, testParentDirectory.getAbsolutePath(),
        serverJarPaths, namespaceFragment, pluginsFragment, securityRootDirectory, tcProperties, serverPropertiesSupplier.get(),
        serverDebugStartPort, clusterSize, configureCluster, clusterName, logConfig, clientReconnectWindowTime,
        failoverPriorityVoterCount);

    this.interlock = extendedHarnessEntry.getInterlock();
    this.portLocks = extendedHarnessEntry.getPortLocks();
    this.stateManager = extendedHarnessEntry.getStateManager();
    this.clusterStripes = extendedHarnessEntry.getStripes();
    this.clusterControl = extendedHarnessEntry.getProcessControl();

    // Spin up an extra thread to call waitForFinish on the stateManager.
    // This is required since galvan expects that the client is running in a different thread (different process, usually)
    // than the framework, and the framework waits for the finish so that it can terminate the clients/servers if any of
    // them trigger an unexpected failure.
    // Without this, the client will hang in the case when the server crashes since nobody is running the logic to detect
    // that.
    Assert.assertTrue(null == this.shepherdingThread);
    this.shepherdingThread = new Thread(){
      @Override
      public void run() {
        setSafeForRun(true);
        boolean didPass = false;
        try {
          stateManager.waitForFinish();
          didPass = true;
        } catch (GalvanFailureException e) {
          didPass = false;
        }
        // Whether we passed or failed, bring everything down.
        try {
          interlock.forceShutdown();
        } catch (GalvanFailureException e) {
          e.printStackTrace();
          didPass = false;
        }
        setSafeForRun(false);

        portLocks.close();

        if (!didPass) {
          // Typically, we want to interrupt the thread running as the "client" as it might be stuck in a connection
          // attempt, etc.  When Galvan is run in the purely multi-process mode, this is typically where all
          // sub-processes would be terminated.  Since we are running the client as another thread, in-process, the
          // best we can do is interrupt it from a lower-level blocking call.
          // NOTE:  the "client" is also the thread which created us and will join on our termination, before
          // returning back to the user code so it is possible that this interruption could be experienced in its
          // join() call (in which case, we can safely ignore it).
          isInterruptingClient = true;
          clientThread.interrupt();
        }
      }
    };
    this.shepherdingThread.setName("Shepherding Thread");
    this.shepherdingThread.start();
    waitForSafe();
  }

  private File createTempFolder() {
    int idx = 1;
    while (true) {
      File dir = new File(clusterDirectory, displayName + "#" + idx++);
      if (dir.exists()) {
        continue;
      }
      if (!dir.mkdirs()) {
        throw new IllegalStateException("Unable to create temporary folder " + dir);
      }
      // galvan will append something like: \stripe1\testServer100\server\bin\start-tc-server.bat (53 chars)
      if(IS_WIN && dir.getAbsolutePath().length() + 53 > 255) {
        throw new IllegalStateException("Please reduce your test name or test class name. The folder " + dir.getAbsolutePath() + " is too long to be able to append the server startup script");
      }
      return dir;
    }
  }

  public void manualStop() {
    internalStop();
  }

  @Override
  protected void after() {
    internalStop();
  }

  private void internalStop() {
    stateManager.setTestDidPassIfNotFailed();
    // NOTE:  The waitForFinish is called by the shepherding thread so we just join on it having done that.
    try {
      this.shepherdingThread.join();
    } catch (InterruptedException ignorable) {
      // Note that we both need to join on the shepherding thread (since we created it) but it also tries to interrupt
      // us in the case where we are stuck somewhere else so this exception is possible.
      // This confusion is part of the double-duty being done by the thread from the test harness:  running Galvan
      // _and_ the test.  We split off the Galvan duty to the shepherding thread, so that the test thread can run the
      // test, but we still need to re-join, at the end.
      Assert.assertTrue(this.isInterruptingClient);
      // Clear this flag.
      this.isInterruptingClient = false;
      try {
        this.shepherdingThread.join();
      } catch (InterruptedException unexpected) {
        // Interrupts are unexpected at this point - fail.
        Assert.fail(unexpected.getLocalizedMessage());
      }
    }
    try {
      if (stateManager.checkDidPass()) {
        removeGalvanTestDir(testParentDirectory, harnessLogger);
      }
    } catch (GalvanFailureException gve) {
      // Ignore
    }
    this.shepherdingThread = null;
  }

  @Override
  public URI getConnectionURI() {
    return URI.create(clusterStripes.get(0).stripeUri);
  }

  @Override
  public URI getStripeConnectionURI(int stripeIndex) {
    return URI.create(clusterStripes.get(stripeIndex).stripeUri.replace("terracotta://", "stripe://"));
  }

  @Override
  public URI getClusterConnectionURI() {
    return URI.create(clusterStripes.get(0).stripeUri.replace("terracotta://", "cluster://"));
  }

  @Override
  public String[] getClusterHostPorts() {
    List<String> hostPorts = new ArrayList<>();
    for (ReadyStripe stripe : clusterStripes) {
      hostPorts.addAll(Arrays.asList(stripe.stripeUri.substring("terracotta://".length()).split(",")));
    }
    return hostPorts.toArray(new String[hostPorts.size()]);
  }

  @Override
  public Connection newConnection() throws ConnectionException {
    if (!checkSafe()) {
      throw new ConnectionException(null);
    }
    return ConnectionFactory.connect(getConnectionURI(), new Properties());
  }

  @Override
  public IClusterControl getClusterControl() {
    return new IClusterControl() {
      @Override
      public void waitForActive() throws Exception {
        clusterControl.waitForActive();
      }

      @Override
      public void waitForRunningPassivesInStandby() throws Exception {
        clusterControl.waitForRunningPassivesInStandby();
      }

      @Override
      public void startOneServer() throws Exception {
        clusterControl.startOneServer();
      }

      @Override
      public void startAllServers() throws Exception {
        clusterControl.startAllServers();
      }

      @Override
      public void terminateActive() throws Exception {
        clusterControl.terminateActive();
      }

      @Override
      public void terminateOnePassive() throws Exception {
        clusterControl.terminateOnePassive();
      }

      @Override
      public void terminateAllServers() throws Exception {
        clusterControl.terminateAllServers();
      }
    };
  }

  @Override
  public IClusterControl getStripeControl(final int stripeIndex) {
    return new IClusterControl() {
      @Override
      public void waitForActive() throws Exception {
        clusterStripes.get(stripeIndex).stripeControl.waitForActive();
      }

      @Override
      public void waitForRunningPassivesInStandby() throws Exception {
        clusterStripes.get(stripeIndex).stripeControl.waitForRunningPassivesInStandby();
      }

      @Override
      public void startOneServer() throws Exception {
        clusterStripes.get(stripeIndex).stripeControl.startOneServer();
      }

      @Override
      public void startAllServers() throws Exception {
        clusterStripes.get(stripeIndex).stripeControl.startAllServers();
      }

      @Override
      public void terminateActive() throws Exception {
        clusterStripes.get(stripeIndex).stripeControl.terminateActive();
      }

      @Override
      public void terminateOnePassive() throws Exception {
        clusterStripes.get(stripeIndex).stripeControl.terminateOnePassive();
      }

      @Override
      public void terminateAllServers() throws Exception {
        clusterStripes.get(stripeIndex).stripeControl.terminateAllServers();
      }
    };
  }

  private static List<String> convertToStringPaths(List<File> serverJars) {
    List<String> l = new ArrayList<>();
    for (File f : serverJars) {
      l.add(f.getAbsolutePath());
    }
    return l;
  }

  private synchronized void setSafeForRun(boolean isSafe) {
    // Note that this is called in 2 cases:
    // 1) To state that the shepherding thread is running and we can proceed.
    // 2) To state that there was a problem and we can't proceed.
    this.isSafe = isSafe;
    this.notifyAll();
  }

  private synchronized void waitForSafe() {
    boolean interrupted = false;
    while (!interrupted && !this.isSafe) {
      try {
        wait();
      } catch (InterruptedException e) {
        interrupted = true;
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  private synchronized boolean checkSafe() {
    return this.isSafe;
  }

  public boolean checkForFailure() throws GalvanFailureException {
    return stateManager.checkDidPass();
  }

  public void waitForFinish() throws GalvanFailureException {
    stateManager.waitForFinish();
  }

  public void ignoreServerCrashes(boolean ignoreCrash) {
    interlock.ignoreServerCrashes(ignoreCrash);
  }

  // Silently attempts to delete a directory
  private static void removeGalvanTestDir(File dirToDelete, VerboseLogger logger) {
    if (System.getProperty("galvan.noclean") == null) {
      try (Stream<Path> pathStream = Files.walk(dirToDelete.toPath())) {
        pathStream
            .sorted(Comparator.reverseOrder())
            .map(Path::toFile)
            .forEach(File::delete);
      } catch (IOException e) {
        logger.error("Unable to clean test folder " + dirToDelete);
      }
    }
  }
}
