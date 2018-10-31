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

import com.terracottatech.br.ssi.BackupCapable;
import com.terracottatech.config.data_roots.DataDirectoriesConfig;
import com.terracottatech.config.data_roots.DataDirectoriesConfigImpl;
import com.terracottatech.data.config.DataDirectories;
import com.terracottatech.data.config.DataRootMapping;

import com.terracottatech.utilities.DirtydbCleaner;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.terracotta.entity.PlatformConfiguration;
import org.terracotta.offheapresource.OffHeapResource;
import org.terracotta.offheapresource.OffHeapResourceIdentifier;
import org.terracotta.offheapresource.OffHeapResources;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.terracottatech.ehcache.clustered.server.services.frs.EhcacheFRSServiceProvider.EHCACHE_FRS_STORAGE_SPACE;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author vmad
 */
public class EhcacheFRSServiceProviderTest {

  private static final String DATA_ROOT_ID = "root";
  private static final String SERVER_NAME = "server";

  private String DATA_ROOT_PATH;
  private PlatformConfiguration platformConfigurationMock;
  private TestOffHeapResources offHeapResources;

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    DATA_ROOT_PATH = folder.newFolder(SERVER_NAME).getAbsolutePath();

    DataDirectories dataDirectories = new DataDirectories();
    DataRootMapping dataRootMapping = new DataRootMapping();
    dataRootMapping.setName(DATA_ROOT_ID); dataRootMapping.setValue(DATA_ROOT_PATH);
    dataDirectories.getDirectory().add(dataRootMapping);

    offHeapResources = new TestOffHeapResources();
    platformConfigurationMock = mock(PlatformConfiguration.class);
    when(platformConfigurationMock.getServerName()).thenReturn(SERVER_NAME);
    when(platformConfigurationMock.getExtendedConfiguration(DataDirectoriesConfig.class)).thenReturn((Collections.singleton(new DataDirectoriesConfigImpl(null, dataDirectories))));
    when(platformConfigurationMock.getExtendedConfiguration(OffHeapResources.class)).thenReturn((Collections.singleton(offHeapResources)));
  }

  @Test
  public void testLargeInstallation() throws Exception {
    EhcacheFRSServiceProvider ehcacheFRSServiceProvider = new EhcacheFRSServiceProvider();
    offHeapResources.setCapacity(4L * 1024 * 1024 * 1024);
    ehcacheFRSServiceProvider.initialize(null, platformConfigurationMock);
    assertThat(ehcacheFRSServiceProvider.isLargeInstallation(), is(true));
  }

  @Test
  public void testLargeInstallationWithMultipleIdentifiers() throws Exception {
    EhcacheFRSServiceProvider ehcacheFRSServiceProvider = new EhcacheFRSServiceProvider();
    offHeapResources.setCapacity((long)1024 * 1024 * 1024);
    offHeapResources.setIdentifiers(5);
    ehcacheFRSServiceProvider.initialize(null, platformConfigurationMock);
    assertThat(ehcacheFRSServiceProvider.isLargeInstallation(), is(true));
  }

  @Test
  public void testSmallInstallation() throws Exception {
    EhcacheFRSServiceProvider ehcacheFRSServiceProvider = new EhcacheFRSServiceProvider();
    offHeapResources.setCapacity(2L * 1024 * 1024 * 1024);
    ehcacheFRSServiceProvider.initialize(null, platformConfigurationMock);
    assertThat(ehcacheFRSServiceProvider.isLargeInstallation(), is(false));
  }

  @Test
  public void testServiceIsNotBackupCapableWhenNoEhcacheFRSService() throws Exception {
    EhcacheFRSServiceProvider ehcacheFRSServiceProvider = new EhcacheFRSServiceProvider();
    ehcacheFRSServiceProvider.initialize(null, platformConfigurationMock);
    BackupCapable backupCapable = ehcacheFRSServiceProvider.getService(1L, () -> BackupCapable.class);
    assertNull(backupCapable);
  }

  @Test
  public void testServiceIsBackupCapableEhcacheFRSService() throws Exception {
    EhcacheFRSServiceProvider ehcacheFRSServiceProvider = new EhcacheFRSServiceProvider();
    ehcacheFRSServiceProvider.initialize(null, platformConfigurationMock);
    EhcacheFRSService service = ehcacheFRSServiceProvider.getService(1L, new EhcacheFRSServiceConfiguration(null));
    assertNotNull(service);
    BackupCapable backupCapable = ehcacheFRSServiceProvider.getService(1L, () -> BackupCapable.class);
    assertNotNull(backupCapable);
  }

  @Test
  public void testSmallInstallationWithMultipleIdentifiers() throws Exception {
    EhcacheFRSServiceProvider ehcacheFRSServiceProvider = new EhcacheFRSServiceProvider();
    offHeapResources.setCapacity(2L * 1024 * 1024);
    offHeapResources.setIdentifiers(10);
    ehcacheFRSServiceProvider.initialize(null, platformConfigurationMock);
    assertThat(ehcacheFRSServiceProvider.isLargeInstallation(), is(false));
  }

  @Test
  public void testEmptyOffHeapResources() throws Exception {
    EhcacheFRSServiceProvider ehcacheFRSServiceProvider = new EhcacheFRSServiceProvider();
    offHeapResources.setCapacity(0);
    offHeapResources.setIdentifiers(0);
    ehcacheFRSServiceProvider.initialize(null, platformConfigurationMock);
    assertThat(ehcacheFRSServiceProvider.isLargeInstallation(), is(false));
  }

  @Test
  public void prepareForSynchronizationWithEhcacheStorageDir() throws Exception {

    final String TMP_DIRECTORY_NAME = "tmpDirectory";
    final Path tmpDirectory = Paths.get(DATA_ROOT_PATH).resolve(SERVER_NAME).resolve(EHCACHE_FRS_STORAGE_SPACE).resolve(TMP_DIRECTORY_NAME);
    Files.createDirectories(tmpDirectory);

    EhcacheFRSServiceProvider ehcacheFRSServiceProvider = new EhcacheFRSServiceProvider();
    ehcacheFRSServiceProvider.initialize(null, platformConfigurationMock);

    assertTrue(Files.exists(tmpDirectory));
    ehcacheFRSServiceProvider.prepareForSynchronization();
    assertFalse(Files.exists(tmpDirectory));

    List<Path> backupDirectories = getBackupDirectories();
    assertThat(backupDirectories.size(), is(1));
    Path backupDirectory = backupDirectories.get(0).resolve(EHCACHE_FRS_STORAGE_SPACE);
    assertTrue(Files.exists(backupDirectory.resolve(TMP_DIRECTORY_NAME)));
  }

  @Test
  public void prepareForSynchronizationWithEhcacheStorageDirWithBackupLimit() throws Exception {
    final String TMP_DIRECTORY_NAME = "tmpDirectory";
    final Path tmpDirectory = Paths.get(DATA_ROOT_PATH).resolve(SERVER_NAME).resolve(EHCACHE_FRS_STORAGE_SPACE).resolve(TMP_DIRECTORY_NAME);
    final int defaultDirtydbBackupLimit = DirtydbCleaner.DEFAULT_DIRTYDB_BACKUPS_LIMIT;
    for(int i = 0; i < defaultDirtydbBackupLimit + 1; i++) {
      Files.createDirectories(tmpDirectory);
      EhcacheFRSServiceProvider ehcacheFRSServiceProvider = new EhcacheFRSServiceProvider();
      ehcacheFRSServiceProvider.initialize(null, platformConfigurationMock);
      assertTrue(Files.exists(tmpDirectory));
      ehcacheFRSServiceProvider.prepareForSynchronization();
      assertFalse(Files.exists(tmpDirectory));
      Thread.sleep(1000); // need to slow down a bit otherwise next backup dir name will be same as current one
    }

    assertThat("Backup dir(s) count after prepareForSynchronization call is not correct!",
        getBackupDirectories().size(), is(defaultDirtydbBackupLimit));
  }

  @Test
  public void prepareForSynchronizationWithoutEhcacheStorageDir() throws Exception {
    //this shouldn't throw any exceptions
    EhcacheFRSServiceProvider ehcacheFRSServiceProvider = new EhcacheFRSServiceProvider();
    ehcacheFRSServiceProvider.initialize(null, platformConfigurationMock);
    ehcacheFRSServiceProvider.prepareForSynchronization();
  }

  @Test
  public void prepareForSynchronizationWithProblematicServerName() throws Exception {
    String serverName = "server:9540";
    final String TMP_DIRECTORY_NAME = "tmpDirectory";

    when(platformConfigurationMock.getServerName()).thenReturn(serverName);

    serverName = DataDirectoriesConfig.cleanStringForPath(serverName);
    final Path tmpDirectory = Paths.get(DATA_ROOT_PATH).resolve(serverName).resolve(EHCACHE_FRS_STORAGE_SPACE).resolve(TMP_DIRECTORY_NAME);
    Files.createDirectories(tmpDirectory);

    EhcacheFRSServiceProvider ehcacheFRSServiceProvider = new EhcacheFRSServiceProvider();
    ehcacheFRSServiceProvider.initialize(null, platformConfigurationMock);

    assertTrue(Files.exists(tmpDirectory));
    ehcacheFRSServiceProvider.prepareForSynchronization();
    assertFalse(Files.exists(tmpDirectory));
  }

  private List<Path> getBackupDirectories() throws IOException {
    try (Stream<Path> list = Files.list(Paths.get(DATA_ROOT_PATH).resolve(SERVER_NAME))) {
      return list
          .filter((p) -> p.getFileName().toString().startsWith(EhcacheFRSServiceProvider.BACKUP_DIR_PREFIX))
          .collect(Collectors.toList());
    }
  }

  private static final class TestOffHeapResources implements OffHeapResources {
    private long perIdentifierCapacity = 1024 * 1024;
    private Set<OffHeapResourceIdentifier> idSet = Collections.singleton(OffHeapResourceIdentifier.identifier("test"));

    void setCapacity(long capacity) {
      perIdentifierCapacity = capacity;
    }

    void setIdentifiers(int count) {
      if (count <= 0) {
        idSet = Collections.emptySet();
      } else {
        Set<OffHeapResourceIdentifier> localSet = new HashSet<>();
        for (int i = 0; i < count; i++) {
          localSet.add(OffHeapResourceIdentifier.identifier("test" + i));
        }
        idSet = Collections.unmodifiableSet(localSet);
      }
    }

    @Override
    public Set<OffHeapResourceIdentifier> getAllIdentifiers() {
      return idSet;
    }

    @Override
    public OffHeapResource getOffHeapResource(OffHeapResourceIdentifier offHeapResourceIdentifier) {
      if (idSet.contains(offHeapResourceIdentifier)) {
        return new OffHeapResource() {
          @Override
          public boolean reserve(long l) throws IllegalArgumentException {
            return false;
          }

          @Override
          public void release(long l) throws IllegalArgumentException {
          }

          @Override
          public long available() {
            return 0;
          }

          @Override
          public long capacity() {
            return perIdentifierCapacity;
          }
        };
      } else {
        return null;
      }
    }
  }
}