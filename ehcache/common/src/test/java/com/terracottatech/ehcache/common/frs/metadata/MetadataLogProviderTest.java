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
package com.terracottatech.ehcache.common.frs.metadata;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.terracottatech.ehcache.common.frs.RootPathProvider;

import java.io.File;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

/**
 * Test {@link MetadataProvider} interfaces.
 *
 * @author RKAV
 */
public class MetadataLogProviderTest {
  private static final String DEFAULT_CONTAINER = "default-container";
  private static final String DEFAULT_ROOT = "default-root";
  private static final String DATA_LOG_NAME1 = "frs1";
  private static final String DATA_LOG_NAME2 = "frs2";
  private static final String DEFAULT_CONTAINER2 = "default-container2";
  private static final String DEFAULT_ROOT2 = "default-root2";

  private static final String[] ROOT_NAMES = {"root1", "root2"};
  private static final String[] L1_NAMES = {"l1-n1", "l1-n2"};
  private static final String[] L2_NAMES = {"l2-n1", "l2-n2"};
  private static final String[] L3_NAMES = {"l3-n1", "l3-n2"};


  // threshold value above which stuff is in second container
  private static final int THRESHOLD_VALUE = 10000;

  private static final FrsContainerIdentifier CONTAINER_ID1 =
      new FrsContainerIdentifier(DEFAULT_ROOT, DEFAULT_CONTAINER);
  private static final FrsContainerIdentifier CONTAINER_ID2 =
      new FrsContainerIdentifier(DEFAULT_ROOT2, DEFAULT_CONTAINER2);

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private File folder;
  private File folder1;

  @Before
  public void setUp() throws Exception {
    folder = temporaryFolder.newFolder();
    folder1 = temporaryFolder.newFolder();
  }

  @Test
  public void testSimpleMetadataProvider() throws Exception {
    MetadataProvider<String, TestRootConfiguration, TestChildConfiguration> metadataProvider =
        buildProvider(false, 1);
    metadataProvider.bootstrapAllContainers();
    addNLevel(metadataProvider, false);

    Map<String, TestRootConfiguration> rootMap = metadataProvider.getAllRootConfiguration();
    assertThat(rootMap.size(), is(ROOT_NAMES.length));
    assertThat(rootMap.get(ROOT_NAMES[0]), is(new TestRootConfiguration(ROOT_NAMES[0], 1)));

    metadataProvider.removeRootConfigurationFrom(CONTAINER_ID1, ROOT_NAMES[1]);

    assertChildren(metadataProvider, 1);
    metadataProvider.shutdownAllContainers();

    // now bootstrap again to see if metadata exists
    metadataProvider = buildProvider(false, 1);
    metadataProvider.bootstrapAllContainers();
    assertChildren(metadataProvider, 1);

    metadataProvider.shutdownAllContainers();
  }

  @Test
  public void testSimpleMetadataProviderWithAdditionalTypes() throws Exception {
    MetadataProvider<String, TestRootConfiguration, TestChildConfiguration>
        metadataProvider = buildProvider(true, 1);
    metadataProvider.bootstrapAllContainers();

    addNLevel(metadataProvider, true);

    Map<String, TestChildConfiguration> m = metadataProvider.getFirstLevelChildConfigurationByRoot(ROOT_NAMES[0]);
    assertThat(m.size(), is(2));

    String l2_child_to_remove = ROOT_NAMES[0] + "-" + L1_NAMES[0] + "-" + L2_NAMES[1];
    String l3_child_removed = l2_child_to_remove + "-" + L3_NAMES[0];
    String l2_child_existing = ROOT_NAMES[0] + "-" + L1_NAMES[1] + "-" + L2_NAMES[1];

    metadataProvider.removeNthLevelOtherChildConfigurationFrom(CONTAINER_ID1, l2_child_to_remove, TestChildConfiguration1.class);
    assertAfterRemove(metadataProvider, l2_child_to_remove, l3_child_removed, l2_child_existing);
    metadataProvider.shutdownAllContainers();

    // now bootstrap again to see if metadata exists
    metadataProvider = buildProvider(true, 1);
    metadataProvider.bootstrapAllContainers();
    assertAfterRemove(metadataProvider, l2_child_to_remove, l3_child_removed, l2_child_existing);
    metadataProvider.shutdownAllContainers();
  }

  @Test
  public void testMultiContainerMetadataProvider() throws Exception {
    MetadataProvider<String, TestRootConfiguration, TestChildConfiguration>
        metadataProvider = buildProvider(true, 2);

    metadataProvider.bootstrapAllContainers();
    metadataProvider.addRootConfiguration(ROOT_NAMES[0], new TestRootConfiguration(ROOT_NAMES[0], 2));
    metadataProvider.addRootConfiguration(ROOT_NAMES[1], new TestRootConfiguration(ROOT_NAMES[1], 1));

    Map<String, TestRootConfiguration> rootMap = metadataProvider.getAllRootConfiguration();
    assertThat(rootMap.size(), is(2));
    assertThat(rootMap.get(ROOT_NAMES[0]).containerIds.contains(CONTAINER_ID1), is(true));
    assertThat(rootMap.get(ROOT_NAMES[0]).containerIds.size(), is(2));

    metadataProvider.removeRootConfigurationFrom(CONTAINER_ID1, ROOT_NAMES[1]);
    rootMap = metadataProvider.getAllRootConfiguration();
    assertThat(rootMap.size(), is(1));

    String child1 = ROOT_NAMES[0] + "-" + L1_NAMES[0];
    String child2 = ROOT_NAMES[0] + "-" + L1_NAMES[1];

    metadataProvider.addFirstLevelChildConfiguration(child1, new TestChildConfiguration(ROOT_NAMES[0], 11111));
    metadataProvider.addFirstLevelChildConfiguration(child2, new TestChildConfiguration(ROOT_NAMES[0], 1211));
    Map<String, TestChildConfiguration> m = metadataProvider.getFirstLevelChildConfigurationByRoot(ROOT_NAMES[0]);
    assertThat(m.size(), is(2));
    assertThat(m.get(child1).someValue, is(11111));
    assertThat(m.get(child2).someValue, is(1211));
    metadataProvider.shutdownAllContainers();

    // now bootstrap again to see if metadata exists
    metadataProvider = buildProvider(true, 2);
    metadataProvider.bootstrapAllContainers();

    m = metadataProvider.getFirstLevelChildConfigurationByRoot(ROOT_NAMES[0]);
    assertThat(m.size(), is(2));
    assertThat(m.get(child1).someValue, is(11111));
    assertThat(m.get(child2).someValue, is(1211));

    metadataProvider.shutdownAllContainers();
  }

  private MetadataProvider<String, TestRootConfiguration, TestChildConfiguration>
  buildProvider(boolean withOtherTypes, int numContainers) {
    MetadataProviderBuilder<String, TestRootConfiguration, TestChildConfiguration> metadataBuilder =
        MetadataProviderBuilder.metadata(new TestPathProvider(numContainers),
            String.class, TestRootConfiguration.class,
            TestChildConfiguration.class);
    if (withOtherTypes) {
      metadataBuilder = metadataBuilder.withOtherChildType(TestChildConfiguration.class);
      metadataBuilder = metadataBuilder.withOtherChildType(TestChildConfiguration1.class);
    }
    return metadataBuilder.build();
  }

  private void addNLevel(MetadataProvider<String, TestRootConfiguration, TestChildConfiguration> metadataProvider,
                         boolean otherTypes) {
    for (String l0 : ROOT_NAMES) {
      metadataProvider.addRootConfiguration(l0, new TestRootConfiguration(l0, 1));
      for (String l1 : L1_NAMES) {
        String l1_child = l0 + "-" + l1;
        metadataProvider.addFirstLevelChildConfiguration(l1_child, new TestChildConfiguration(l0, 111));
        if (otherTypes) {
          for (String l2 : L2_NAMES) {
            String l2_child = l1_child + "-" + l2;
            metadataProvider.addNthLevelOtherChildConfiguration(l2_child,
                new TestChildConfiguration(l1_child, 211), TestChildConfiguration.class);
            metadataProvider.addNthLevelOtherChildConfiguration(l2_child,
                new TestChildConfiguration1(l1_child, 212), TestChildConfiguration1.class);
            for (String l3 : L3_NAMES) {
              String l3_child = l2_child + "-" + l3;
              metadataProvider.addNthLevelOtherChildConfiguration(l3_child,
                  new TestChildConfiguration(l2_child, 311), TestChildConfiguration.class);
              metadataProvider.addNthLevelOtherChildConfiguration(l3_child,
                  new TestChildConfiguration1(l2_child, 312), TestChildConfiguration1.class);
            }
          }
        }
      }
    }
  }

  private void assertChildren(MetadataProvider<String, TestRootConfiguration, TestChildConfiguration> metadataProvider,
                              int removedRoot) {
    String existingRoot = ROOT_NAMES[(removedRoot + 1) % ROOT_NAMES.length];
    Map<String, TestRootConfiguration> rootMap = metadataProvider.getAllRootConfiguration();
    assertThat(rootMap.size(), is(1));
    assertNull(rootMap.get(ROOT_NAMES[removedRoot]));
    assertThat(rootMap.get(existingRoot), is(new TestRootConfiguration(existingRoot, 1)));
    Map<String, TestChildConfiguration> m = metadataProvider.getFirstLevelChildConfigurationByRoot(existingRoot);
    assertThat(m.size(), is(2));
    assertThat(m.get(existingRoot + "-" + L1_NAMES[0]).someValue, is(111));
    assertThat(m.get(existingRoot + "-" + L1_NAMES[1]).someValue, is(111));
  }

  private void assertAfterRemove(MetadataProvider<String, TestRootConfiguration, TestChildConfiguration> metadataProvider,
                                 String removedChild, String childOfRemovedChild, String existingChild) {
    Map<String, TestChildConfiguration> m = metadataProvider.getNthLevelOtherChildConfigurationByParent(existingChild,
        TestChildConfiguration.class);
    assertThat(m.size(), is(2));
    Map<String, TestChildConfiguration1> m1 = metadataProvider.getNthLevelOtherChildConfigurationByParent(removedChild,
        TestChildConfiguration1.class);
    assertThat(m1.size(), is(0));
    assertNull(metadataProvider.getNthLevelOtherChildConfigEntry(CONTAINER_ID1, childOfRemovedChild, TestChildConfiguration1.class));
    assertThat(metadataProvider.getNthLevelOtherChildConfigEntry(CONTAINER_ID1, existingChild, TestChildConfiguration1.class),
        Matchers.<ChildConfiguration<String>>is(new TestChildConfiguration1(ROOT_NAMES[0] + "-" + L1_NAMES[1], 212)));
  }

  private static class TestRootConfiguration implements Serializable, RootConfiguration {
    private static final long serialVersionUID = 6053370216995173871L;
    private int objectIndex = 0;
    private final Set<FrsContainerIdentifier> containerIds;
    private final String alias;

    private TestRootConfiguration(String alias, int count) {
      containerIds = (count <= 1) ?
          Collections.singleton(CONTAINER_ID1) :
          new HashSet<>(Arrays.asList(CONTAINER_ID1, CONTAINER_ID2));

      this.alias = alias;
    }

    @Override
    public Set<FrsContainerIdentifier> getContainers() {
      return containerIds;
    }

    @Override
    public int getObjectIndex() {
      return objectIndex;
    }

    @Override
    public void setObjectIndex(int index) {
      this.objectIndex = index;
    }

    @Override
    public void validate(MetadataConfiguration newMetadataConfiguration) {
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      TestRootConfiguration that = (TestRootConfiguration)o;

      return alias.equals(that.alias);

    }

    @Override
    public int hashCode() {
      return alias.hashCode();
    }
  }

  private static class TestChildConfiguration implements Serializable, ChildConfiguration<String> {
    private static final long serialVersionUID = -8366198097936879980L;
    private final String parentId;
    private final int someValue;
    private final FrsDataLogIdentifier dataLogId;

    private int objectIndex = 0;

    TestChildConfiguration(String parentId, int someValue) {
      this.parentId = parentId;
      this.someValue = someValue;
      this.dataLogId = (someValue <= THRESHOLD_VALUE) ?
          new FrsDataLogIdentifier(DEFAULT_ROOT, DEFAULT_CONTAINER, DATA_LOG_NAME1):
          new FrsDataLogIdentifier(DEFAULT_ROOT2, DEFAULT_CONTAINER2, DATA_LOG_NAME2);
    }

    @Override
    public int getObjectIndex() {
      return objectIndex;
    }

    @Override
    public void setObjectIndex(int index) {
      this.objectIndex = index;
    }

    @Override
    public void validate(MetadataConfiguration newMetadataConfiguration) {
    }

    @Override
    public String getParentId() {
      return parentId;
    }

    @Override
    public FrsDataLogIdentifier getDataLogIdentifier() {
      return dataLogId;
    }
  }

  private static class TestChildConfiguration1 implements Serializable, ChildConfiguration<String> {
    private static final long serialVersionUID = -2312600312857810607L;
    private final String parentId;
    private final int someValue;
    private final FrsDataLogIdentifier dataLogId;

    private int objectIndex = 0;

    TestChildConfiguration1(String parentId, int someValue) {
      this.parentId = parentId;
      this.someValue = someValue;
      this.dataLogId = new FrsDataLogIdentifier(DEFAULT_ROOT, DEFAULT_CONTAINER, DATA_LOG_NAME2);
    }

    @Override
    public int getObjectIndex() {
      return objectIndex;
    }

    @Override
    public void setObjectIndex(int index) {
      this.objectIndex = index;
    }

    @Override
    public void validate(MetadataConfiguration newMetadataConfiguration) {
    }

    @Override
    public String getParentId() {
      return parentId;
    }

    @Override
    public FrsDataLogIdentifier getDataLogIdentifier() {
      return dataLogId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      TestChildConfiguration1 that = (TestChildConfiguration1)o;

      return someValue == that.someValue && parentId.equals(that.parentId) && dataLogId.equals(that.dataLogId);
    }

    @Override
    public int hashCode() {
      int result = parentId.hashCode();
      result = 31 * result + someValue;
      result = 31 * result + dataLogId.hashCode();
      return result;
    }
  }

  private class TestPathProvider implements RootPathProvider {
    private final int numContainers;

    TestPathProvider(int numContainers) {
      this.numContainers = numContainers;
    }

    @Override
    public File getRootPath(String rootName) {
      return (rootName.equals(DEFAULT_ROOT)) ? folder : folder1;
    }

    @Override
    public File getConfiguredRoot(String rootName) {
      return getRootPath(rootName);
    }

    @Override
    public Set<File> getAllExistingContainers() {
      return (numContainers <= 1) ?
          Collections.singleton(new File(folder, DEFAULT_CONTAINER)) :
          new HashSet<>(Arrays.asList(new File(folder, DEFAULT_CONTAINER),
              new File(folder1, DEFAULT_CONTAINER2)));
    }
  }
}
