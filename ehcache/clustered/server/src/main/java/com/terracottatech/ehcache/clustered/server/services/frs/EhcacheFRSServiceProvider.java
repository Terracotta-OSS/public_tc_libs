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

package com.terracottatech.ehcache.clustered.server.services.frs;

import com.tc.classloader.BuiltinService;
import com.terracottatech.br.ssi.BackupCapable;
import com.terracottatech.config.data_roots.DataDirectoriesConfig;
import com.terracottatech.config.data_roots.DataDirectories;
import com.terracottatech.ehcache.clustered.server.services.frs.impl.EhcacheFRSServiceImpl;
import com.terracottatech.ehcache.clustered.server.services.frs.management.Management;
import com.terracottatech.ehcache.clustered.server.services.frs.management.RestartStoreBinding;
import com.terracottatech.frs.Statistics;
import com.terracottatech.utilities.DirtydbCleaner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.entity.PlatformConfiguration;
import org.terracotta.entity.ServiceConfiguration;
import org.terracotta.entity.ServiceProvider;
import org.terracotta.entity.ServiceProviderCleanupException;
import org.terracotta.entity.ServiceProviderConfiguration;
import org.terracotta.entity.ServiceRegistry;
import org.terracotta.entity.StateDumpCollector;
import org.terracotta.management.service.monitoring.ManageableServerComponent;
import org.terracotta.offheapresource.OffHeapResource;
import org.terracotta.offheapresource.OffHeapResources;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

/**
 * @author vmad
 */
@BuiltinService
public class EhcacheFRSServiceProvider implements ServiceProvider, Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(EhcacheFRSServiceProvider.class);
  private static final long LARGE_OFFHEAP = 4L * 1024 * 1024 * 1024;

  private static final Path EHCACHE_STORAGE_SPACE = Paths.get("ehcache");
  public static final Path EHCACHE_FRS_STORAGE_SPACE = EHCACHE_STORAGE_SPACE.resolve("frs");
  static final String BACKUP_DIR_PREFIX = "terracotta.backup.";
  private EhcacheFRSService ehcacheFRSService;
  private DataDirectories dataDirectories;
  private boolean largeInstallation;
  private final Management management = new Management();

  @Override
  public void addStateTo(StateDumpCollector dump) {
    dump.addState("largeInstallation", String.valueOf(largeInstallation));
    StateDumpCollector restartStoresDump = dump.subStateDumpCollector("restartStores");
    for (RestartStoreBinding binding : management.getBindings()) {
      StateDumpCollector restartStoreDump = restartStoresDump.subStateDumpCollector(binding.getAlias());
      restartStoreDump.addState("id", binding.getRestartableStoreId());
      restartStoreDump.addState("root", binding.getRestartableStoreRoot());
      restartStoreDump.addState("container", binding.getRestartableStoreContainer());
      restartStoreDump.addState("name", binding.getRestartableLogName());
      restartStoreDump.addState("location", binding.getRestartableStorageLocation());
      Statistics statistics = binding.getValue().getStatistics();
      StateDumpCollector statisticsDump = restartStoreDump.subStateDumpCollector("statistics");
      statisticsDump.addState("totalUsage", String.valueOf(statistics.getTotalUsed()));
      statisticsDump.addState("expiredSize", String.valueOf(statistics.getExpiredSize()));
      statisticsDump.addState("liveSize", String.valueOf(statistics.getLiveSize()));
      statisticsDump.addState("totalAvailable", String.valueOf(statistics.getTotalAvailable()));
      statisticsDump.addState("totalRead", String.valueOf(statistics.getTotalRead()));
      statisticsDump.addState("totalWritten", String.valueOf(statistics.getTotalWritten()));
    }
  }

  @Override
  public boolean initialize(ServiceProviderConfiguration serviceProviderConfiguration,
                            PlatformConfiguration platformConfiguration) {
    // construct dataRootConfig object

    // we have a large installation if total capacity exceeds 4GB.
    Collection<OffHeapResources> extendedConfiguration = platformConfiguration.getExtendedConfiguration(OffHeapResources.class);
    largeInstallation = extendedConfiguration.stream()
        .findFirst()
        .map((s) -> s.getAllIdentifiers().stream().mapToLong((id) -> {
          OffHeapResource resource = s.getOffHeapResource(id);
          return (resource != null ? resource.capacity() : 0L);
        }).sum()).orElse(0L) >= LARGE_OFFHEAP;

    Collection<DataDirectoriesConfig> dataDirectoriesConfigs = platformConfiguration.getExtendedConfiguration(DataDirectoriesConfig.class);
    if(dataDirectoriesConfigs.size() > 1) {
      throw new UnsupportedOperationException("There are " + dataDirectoriesConfigs.size() + "DataDirectoriesConfig , this is not supported. " +
                                              "There must be only one!");
    }

    Iterator<DataDirectoriesConfig> dataRootConfigIterator = dataDirectoriesConfigs.iterator();
    if(dataRootConfigIterator.hasNext()) {
      this.dataDirectories = dataRootConfigIterator.next().getDataDirectoriesForServer(platformConfiguration);
      if(this.dataDirectories.getDataDirectoryNames().isEmpty()) {
        throw new UnsupportedOperationException("There are no data-root-configs defined, this is not supported. There must be at least one!");
      }
    } else {
      LOGGER.warn("No data-root-config defined - this will prevent provider from offering any EhcacheFRSServiceProvider.");
    }
    return true;
  }

  @Override
  public synchronized <T> T getService(long l,
                                       ServiceConfiguration<T> serviceConfiguration) {
    if (serviceConfiguration.getServiceType() == ManageableServerComponent.class) {
      return serviceConfiguration.getServiceType().cast(management);
    }
    if (serviceConfiguration instanceof EhcacheFRSServiceConfiguration) {
      if (ehcacheFRSService == null) {
        EhcacheFRSServiceConfiguration configuration = (EhcacheFRSServiceConfiguration) serviceConfiguration;
        ServiceRegistry serviceRegistry = configuration.getServiceRegistry();
        ehcacheFRSService = new EhcacheFRSServiceImpl(dataDirectories, serviceRegistry, largeInstallation, management);
      }
      return serviceConfiguration.getServiceType().cast(ehcacheFRSService);
    }
    if (serviceConfiguration.getServiceType().isAssignableFrom(BackupCapable.class)) {
      if (ehcacheFRSService != null) {
        return serviceConfiguration.getServiceType().cast(ehcacheFRSService.getBackupCoordinator());
      }
    }
    return null;
  }

  @Override
  public Collection<Class<?>> getProvidedServiceTypes() {
    return Arrays.asList(EhcacheFRSService.class, ManageableServerComponent.class, BackupCapable.class);
  }

  @Override
  public void prepareForSynchronization() throws ServiceProviderCleanupException {
    //This implementation moves ${data-root}/ehcache/frs (if exists) into ${data-root}/terracotta.backup.{date&time}/ehcache/
    String backupDirectory = BACKUP_DIR_PREFIX + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd.HHmmss"));
    for (String rootID : this.dataDirectories.getDataDirectoryNames()) {
      Path dataRoot = dataDirectories.getDataDirectory(rootID);
      Path ehcacheFRSStorageSpace = dataRoot.resolve(EHCACHE_FRS_STORAGE_SPACE);
      if(Files.exists(ehcacheFRSStorageSpace)) {
        Path backupFullPath = dataRoot.resolve(backupDirectory).resolve(EHCACHE_STORAGE_SPACE);
        try {
          Files.createDirectories(backupFullPath);
          Files.move(ehcacheFRSStorageSpace, backupFullPath.resolve(ehcacheFRSStorageSpace.getFileName()));
          DirtydbCleaner.deleteOldBackups(dataRoot, BACKUP_DIR_PREFIX);
        } catch (IOException e) {
          throw new ServiceProviderCleanupException("Caught exception while clearing data for EhcacheFRSServiceProvider",
            e);
        }
      }
    }
  }

  //needed in passthrough testing
  @Override
  public void close() throws IOException {
    if(ehcacheFRSService != null) {
      ehcacheFRSService.shutdown();
    }
  }

  boolean isLargeInstallation() {
    return largeInstallation;
  }
}
