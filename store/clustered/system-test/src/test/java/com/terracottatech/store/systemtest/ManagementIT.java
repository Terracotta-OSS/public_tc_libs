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
package com.terracottatech.store.systemtest;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.terracottatech.store.Dataset;
import com.terracottatech.store.DatasetReader;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.Type;
import com.terracottatech.store.common.test.Employee;
import com.terracottatech.store.common.test.Employees;
import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.indexing.IndexSettings;
import com.terracottatech.store.internal.InternalDatasetManager;
import com.terracottatech.store.management.CollectingManageableObserver;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.store.statistics.DatasetOutcomes;
import com.terracottatech.store.statistics.DatasetOutcomes.GetOutcome;
import com.terracottatech.tool.WaitForAssert;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.terracotta.connection.Connection;
import org.terracotta.connection.ConnectionFactory;
import org.terracotta.connection.ConnectionPropertyNames;
import org.terracotta.management.entity.nms.NmsConfig;
import org.terracotta.management.entity.nms.client.DefaultNmsService;
import org.terracotta.management.entity.nms.client.NmsEntity;
import org.terracotta.management.entity.nms.client.NmsEntityFactory;
import org.terracotta.management.model.call.Parameter;
import org.terracotta.management.model.capabilities.Capability;
import org.terracotta.management.model.capabilities.context.CapabilityContext;
import org.terracotta.management.model.capabilities.descriptors.Settings;
import org.terracotta.management.model.capabilities.descriptors.StatisticDescriptor;
import org.terracotta.management.model.cluster.Client;
import org.terracotta.management.model.cluster.Cluster;
import org.terracotta.management.model.cluster.Server;
import org.terracotta.management.model.cluster.ServerEntity;
import org.terracotta.management.model.context.Context;
import org.terracotta.management.model.context.ContextContainer;
import org.terracotta.management.model.message.Message;
import org.terracotta.management.model.notification.ContextualNotification;
import org.terracotta.management.model.stats.ContextualStatistics;
import org.terracotta.management.registry.ManagementRegistry;
import org.terracotta.management.registry.collect.StatisticCollector;

import java.io.File;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.terracottatech.store.UpdateOperation.write;
import static com.terracottatech.store.statistics.DatasetOutcomes.AddOutcome;
import static com.terracottatech.store.statistics.DatasetOutcomes.DeleteOutcome;
import static com.terracottatech.store.statistics.DatasetOutcomes.StreamOutcome;
import static com.terracottatech.store.statistics.DatasetOutcomes.UpdateOutcome;
import static com.terracottatech.tool.WaitForAssert.assertThatEventually;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.core.AllOf.allOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

@SuppressWarnings("try")
public class ManagementIT extends CRUDTests {

  private final ObjectMapper objectMapper = new ObjectMapper()
      .configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true)
      .configure(SerializationFeature.INDENT_OUTPUT, false)
      .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
      .addMixIn(CapabilityContext.class, CapabilityContextMixin.class);


  private final CollectingManageableObserver observer = new CollectingManageableObserver();

  private ManagementRegistry localManagementRegistry;
  private StatisticCollector statisticCollector;
  private Connection managementConnection;
  private DefaultNmsService nmsService;
  private Supplier<org.terracotta.management.model.cluster.ManagementRegistry> remoteManagementRegistry;
  private Supplier<Client> datasetManagerClient;

  @SuppressWarnings("unused")
  @Test
  public void queryContextContainer() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      ContextContainer contextContainer = localManagementRegistry.getContextContainer();
      assertThat(contextContainer.getName()).isEqualTo("datasetManagerName");
      assertThat(contextContainer.getValue()).isNotEmpty();
      assertThat(contextContainer.getSubContexts()).hasSize(1);
      assertThat(contextContainer.getSubContexts().iterator().next().getName()).isEqualTo("datasetInstanceName");
      assertThat(contextContainer.getSubContexts().iterator().next().getValue()).startsWith(getClass().getSimpleName() + "#" + testName.getMethodName() + "-");
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void queryCapabilities() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      Collection<? extends Capability> capabilities = localManagementRegistry.getCapabilities();

      String datasetName = localManagementRegistry.getCapabilities().stream()
          .filter(o -> o.getName().equals("DatasetSettings"))
          .flatMap(o -> o.getDescriptors(Settings.class).stream())
          .filter(descriptor -> "Dataset".equals(descriptor.getString("type")))
          .map(descriptor -> descriptor.getString("datasetName"))
          .findFirst().get();
      String instanceName = localManagementRegistry.getContextContainer().getSubContexts().iterator().next().getValue();
      String instanceId = localManagementRegistry.getCapabilities().stream()
          .filter(o -> o.getName().equals("DatasetSettings"))
          .flatMap(o -> o.getDescriptors(Settings.class).stream())
          .filter(descriptor -> "DatasetManager".equals(descriptor.getString("type")))
          .map(descriptor -> descriptor.getString("instanceId"))
          .findFirst().get();

      JsonNode expected = readJson(datasetManagerType.isEmbedded() ? "capabilities.json" : "remote-capabilities.json");
      ((ObjectNode) expected.at("/0/descriptors/0")).set("datasetName", TextNode.valueOf(datasetName));
      ((ObjectNode) expected.at("/0/descriptors/0")).set("datasetInstanceName", TextNode.valueOf(instanceName));
      ((ObjectNode) expected.at("/0/descriptors/1")).set("instanceId", TextNode.valueOf(instanceId));

      assertThat(toJson(capabilities)).isEqualTo(expected);
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void directStatisticsQuery() throws TimeoutException {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();

      String instanceName = localManagementRegistry.getContextContainer().getSubContexts().iterator().next().getValue();
      Supplier<? extends ContextualStatistics> statisticsSupplier = () -> queryAllStats().filter(o -> o.getContext().get("datasetInstanceName").equals(instanceName)).findFirst().get();

      System.out.println(statisticsSupplier.get().getStatistics());

      assertThatEventually(statisticsSupplier, allOfStats(0L))
              .within(Duration.ofSeconds(10));

      Integer empID = id(0);
      employeeReader.get(empID).get(); // get found
      employeeReader.get(Integer.MAX_VALUE); // get not found
      int newId = Employees.generateUniqueEmpID();
      employeeWriterReader.add(newId, cellSet(20)); // add
      employeeWriterReader.add(newId, cellSet(20)); // duplicate
      employeeWriterReader.update(id(1), write(Employee.NAME).value("aaa"));
      employeeWriterReader.update(Integer.MAX_VALUE, write(Employee.NAME).value("aaa")); // update not found
      employeeReader.records();
      employeeWriterReader.delete(id(0)); // delete
      employeeWriterReader.delete(id(0)); // delete not found

      assertThatEventually(statisticsSupplier, allOfStats(1L))
              .within(Duration.ofSeconds(10));
    }
  }

  @SuppressWarnings({"varargs", "unchecked"})
  private Matcher<ContextualStatistics> allOfStats(long value) {
    return allOf(
            statisticValue(GetOutcome.SUCCESS, is(value)),
            statisticValue(GetOutcome.NOT_FOUND, is(value)),
            statisticValue(AddOutcome.SUCCESS, is(value)),
            statisticValue(AddOutcome.ALREADY_EXISTS, is(value)),
            statisticValue(UpdateOutcome.SUCCESS, is(value)),
            statisticValue(UpdateOutcome.NOT_FOUND, is(value)),
            statisticValue(DeleteOutcome.SUCCESS, is(value)),
            statisticValue(DeleteOutcome.NOT_FOUND, is(value)),
            statisticValue(StreamOutcome.SUCCESS, is(value))
    );
  }

  private Matcher<ContextualStatistics> statisticValue(DatasetOutcomes stat, Matcher<? extends Serializable> matcher) {
    return new TypeSafeMatcher<ContextualStatistics>() {
      @Override
      protected boolean matchesSafely(ContextualStatistics item) {
        return item.getLatestSampleValue(stat.getStatisticName()).map(matcher::matches).orElse(false);
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("statistic with ").appendText(stat.getStatisticName()).appendText(" matching ").appendDescriptionOf(matcher);
      }
    };
  }

  @SuppressWarnings("unused")
  @Test
  public void sendManagementCall() throws Exception {
    try (Dataset<Integer> dataset = getTestDataset()) {
      String datasetManagerName = localManagementRegistry.getContextContainer().getValue();

      assertThat(statisticCollector.isRunning()).isFalse();
      localManagementRegistry.withCapability("StatisticCollectorCapability")
          .call("startStatisticCollector",
              new Parameter(5, long.class.getName()),
              new Parameter(TimeUnit.SECONDS, TimeUnit.class.getName()))
          .on(Context.create("datasetManagerName", datasetManagerName))
          .build()
          .execute()
          .getSingleResult()
          .getValue(); // will throw underlying exception if any occurred
      assertThat(statisticCollector.isRunning()).isTrue();
    }
  }

  @Test(timeout = 10_000)
  public void collectStatistics() throws Exception {
    try (Dataset<Integer> dataset = getTestDataset()) {
      String datasetManagerName = localManagementRegistry.getContextContainer().getValue();

      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      Integer empID = id(0);
      employeeReader.get(empID).get(); // get found
      employeeReader.get(Integer.MAX_VALUE); // get not found
      int newId = Employees.generateUniqueEmpID();
      employeeWriterReader.add(newId, cellSet(20)); // add
      employeeWriterReader.add(newId, cellSet(20)); // duplicate
      employeeWriterReader.update(id(1), write(Employee.NAME).value("aaa")); // update
      employeeWriterReader.update(Integer.MAX_VALUE, write(Employee.NAME).value("aaa")); // update not found
      employeeReader.records();
      employeeWriterReader.delete(id(0)); // delete
      employeeWriterReader.delete(id(0)); // delete not found

      localManagementRegistry.withCapability("StatisticCollectorCapability")
          .call("startStatisticCollector",
              new Parameter(1, long.class.getName()),
              new Parameter(TimeUnit.SECONDS, TimeUnit.class.getName()))
          .on(Context.create("datasetManagerName", datasetManagerName))
          .build()
          .execute()
          .getSingleResult()
          .getValue(); // will throw underlying exception if any occurred

      boolean collected;

      do {
        collected = observer.streamStatistics()
            .flatMap(Collection::stream)
            .anyMatch(stats -> stats.<Long>getLatestSampleValue(GetOutcome.SUCCESS.getStatisticName()).get() == 1
                && stats.<Long>getLatestSampleValue(GetOutcome.NOT_FOUND.getStatisticName()).get() == 1
                && stats.<Long>getLatestSampleValue(AddOutcome.SUCCESS.getStatisticName()).get() == 1
                && stats.<Long>getLatestSampleValue(AddOutcome.ALREADY_EXISTS.getStatisticName()).get() == 1
                && stats.<Long>getLatestSampleValue(UpdateOutcome.SUCCESS.getStatisticName()).get() == 1
                && stats.<Long>getLatestSampleValue(UpdateOutcome.NOT_FOUND.getStatisticName()).get() == 1
                && stats.<Long>getLatestSampleValue(DeleteOutcome.SUCCESS.getStatisticName()).get() == 1
                && stats.<Long>getLatestSampleValue(DeleteOutcome.NOT_FOUND.getStatisticName()).get() == 1
                && stats.<Long>getLatestSampleValue(StreamOutcome.SUCCESS.getStatisticName()).get() == 1);
      } while (!collected && !Thread.currentThread().isInterrupted());

      assertThat(collected).isTrue();
    }
  }

  @Test(timeout = 10_000)
  public void collectNotifications() throws Exception {
    DatasetManager datasetManager = datasetManagerType.getDatasetManager();
    DatasetConfiguration datasetConfiguration = datasetManagerType.getDatasetConfiguration();

    datasetManager.newDataset("toto", Type.STRING, datasetConfiguration);
    datasetManager.getDataset("toto", Type.STRING).close();
    datasetManager.destroyDataset("toto");
    List<ContextualNotification> notifications = observer.streamNotifications()
        .filter(n -> n.getContext().get("datasetName").equals("toto"))
        .collect(Collectors.toList());
    List<String> types = notifications.stream().map(ContextualNotification::getType).collect(Collectors.toList());
    assertThat(notifications).hasSize(2);
    assertThat(types).contains("DATASET_INSTANCE_CREATED", "DATASET_INSTANCE_CLOSED");
  }

  @Test
  public void client_alias_and_tags() {
    assumeFalse(datasetManagerType.isEmbedded());

    assertThat(datasetManagerClient.get().getName()).isEqualTo("Store:" + datasetManagerType.name());
    //TODO: change to "containsExactlyInOrder" when terracotta-platform#470 will be bring into tc-ee (https://github.com/Terracotta-OSS/terracotta-platform/issues/470)
    assertThat(datasetManagerClient.get().getTags()).containsExactlyInAnyOrder("node-1", "webapp-2", "testing");

    org.terracotta.management.model.cluster.ManagementRegistry topologyRegistry = remoteManagementRegistry.get();
    ContextContainer contextContainer = topologyRegistry.getContextContainer();
    assertThat(contextContainer.getValue()).isEqualTo(datasetManagerType.name());
  }

  @Test
  public void remotelyQueryContextContainer() {
    assumeFalse(datasetManagerType.isEmbedded());

    org.terracotta.management.model.cluster.ManagementRegistry topologyRegistry = remoteManagementRegistry.get();

    ContextContainer contextContainer = topologyRegistry.getContextContainer();
    assertThat(contextContainer.getName()).isEqualTo("datasetManagerName");
    assertThat(contextContainer.getValue()).isNotEmpty();
    assertThat(contextContainer.getSubContexts()).hasSize(1);
    assertThat(contextContainer.getSubContexts().iterator().next().getName()).isEqualTo("datasetInstanceName");
    assertThat(contextContainer.getSubContexts().iterator().next().getValue()).startsWith(getClass().getSimpleName() + "#" + testName.getMethodName() + "-");
  }

  @Test(timeout = 15_000)
  public void remotelyQueryCapabilities() throws InterruptedException {
    assumeFalse(datasetManagerType.isEmbedded());

    try (Dataset<Integer> dataset = getTestDataset()) {
      String datasetName = localManagementRegistry.getCapabilities().stream()
          .filter(o -> o.getName().equals("DatasetSettings"))
          .flatMap(o -> o.getDescriptors(Settings.class).stream())
          .filter(descriptor -> "Dataset".equals(descriptor.getString("type")))
          .map(descriptor -> descriptor.getString("datasetName"))
          .findFirst().get();
      String instanceName = localManagementRegistry.getContextContainer().getSubContexts().iterator().next().getValue();
      String instanceId = localManagementRegistry.getCapabilities().stream()
          .filter(o -> o.getName().equals("DatasetSettings"))
          .flatMap(o -> o.getDescriptors(Settings.class).stream())
          .filter(descriptor -> "DatasetManager".equals(descriptor.getString("type")))
          .map(descriptor -> descriptor.getString("instanceId"))
          .findFirst().get();

      org.terracotta.management.model.cluster.ManagementRegistry topologyRegistry;
      do {
        Thread.sleep(1_000);
        topologyRegistry = remoteManagementRegistry.get();
      }
      while (!Thread.currentThread().isInterrupted() && topologyRegistry.getCapability("DatasetSettings").get().getDescriptors().isEmpty());

      JsonNode actual = toJson(topologyRegistry.getCapabilities());

      JsonNode expected = readJson("remote-capabilities.json");
      ((ObjectNode) expected.at("/0/descriptors/0")).set("datasetName", TextNode.valueOf(datasetName));
      ((ObjectNode) expected.at("/0/descriptors/0")).set("datasetInstanceName", TextNode.valueOf(instanceName));
      ((ObjectNode) expected.at("/0/descriptors/1")).set("instanceId", TextNode.valueOf(instanceId));

      assertThat(actual).isEqualTo(expected);
    }
  }

  @Test
  public void remotelySendManagementCall() throws Exception {
    assumeFalse(datasetManagerType.isEmbedded());

    assertThat(statisticCollector.isRunning()).isFalse();

    String datasetManagerName = localManagementRegistry.getContextContainer().getValue();
    Context datasetManagerContext = datasetManagerClient.get().getContext().with("datasetManagerName", datasetManagerName);
    nmsService.startStatisticCollector(datasetManagerContext, 2, TimeUnit.SECONDS).waitForReturn();

    assertThat(statisticCollector.isRunning()).isTrue();
  }

  @Test(timeout = 10_000)
  public void remotelyCollectNotifications() throws Exception {
    assumeFalse(datasetManagerType.isEmbedded());

    DatasetManager datasetManager = datasetManagerType.getDatasetManager();
    DatasetConfiguration datasetConfiguration = datasetManagerType.getDatasetConfiguration();

    datasetManager.newDataset("toto", Type.STRING, datasetConfiguration);
    Dataset<String> dataset = datasetManager.getDataset("toto", Type.STRING);
    dataset.close();
    datasetManager.destroyDataset("toto");

    List<ContextualNotification> totoNotifs = new ArrayList<>();

    while (totoNotifs.size() < 2 && !Thread.currentThread().isInterrupted()) {
      Message message = nmsService.waitForMessage();
      if (message.getType().equals("NOTIFICATION")) {
        List<ContextualNotification> notifications = message.unwrap(ContextualNotification.class);
        for (ContextualNotification notification : notifications) {
          if ("toto".equals(notification.getContext().get("datasetName"))) {
            totoNotifs.add(notification);
          }
        }
      }
    }

    assertThat(totoNotifs).hasSize(2);
    List<String> types = totoNotifs.stream().map(ContextualNotification::getType).collect(Collectors.toList());
    assertThat(types).contains("DATASET_INSTANCE_CREATED", "DATASET_INSTANCE_CLOSED");
  }

  @Test(timeout = 15_000)
  public void remotelyCollectStatistics() throws Exception {
    assumeFalse(datasetManagerType.isEmbedded());

    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();

      String datasetManagerName = localManagementRegistry.getContextContainer().getValue();
      Context datasetManagerContext = datasetManagerClient.get().getContext().with("datasetManagerName", datasetManagerName);
      nmsService.startStatisticCollector(datasetManagerContext, 2, TimeUnit.SECONDS).waitForReturn();

      Integer empID = id(0);
      employeeReader.get(empID).get(); // get found
      employeeReader.get(Integer.MAX_VALUE); // get not found
      int newId = Employees.generateUniqueEmpID();
      employeeWriterReader.add(newId, cellSet(20)); // add
      employeeWriterReader.add(newId, cellSet(20)); // duplicate
      employeeWriterReader.update(id(1), write(Employee.NAME).value("aaa")); // update
      employeeWriterReader.update(Integer.MAX_VALUE, write(Employee.NAME).value("aaa")); // update not found
      employeeReader.records().close();
      employeeWriterReader.delete(id(0)); // delete
      employeeWriterReader.delete(id(0)); // delete not found

      boolean collected = false;
      while (!collected && !Thread.currentThread().isInterrupted()) {
        Message message = nmsService.waitForMessage();
        if (message.getType().equals("STATISTICS")) {
          List<ContextualStatistics> collectedStats = message.unwrap(ContextualStatistics.class);
          collected = collectedStats.stream().peek(System.out::println)
              .anyMatch(stats -> stats.<Long>getLatestSampleValue(GetOutcome.SUCCESS.getStatisticName()).map(v -> v == 1).orElse(false)
                  && stats.<Long>getLatestSampleValue(GetOutcome.NOT_FOUND.getStatisticName()).map(v -> v == 1).orElse(false)
                  && stats.<Long>getLatestSampleValue(AddOutcome.SUCCESS.getStatisticName()).map(v -> v == 1).orElse(false)
                  && stats.<Long>getLatestSampleValue(AddOutcome.ALREADY_EXISTS.getStatisticName()).map(v -> v == 1).orElse(false)
                  && stats.<Long>getLatestSampleValue(UpdateOutcome.SUCCESS.getStatisticName()).map(v -> v == 1).orElse(false)
                  && stats.<Long>getLatestSampleValue(UpdateOutcome.NOT_FOUND.getStatisticName()).map(v -> v == 1).orElse(false)
                  && stats.<Long>getLatestSampleValue(DeleteOutcome.SUCCESS.getStatisticName()).map(v -> v == 1).orElse(false)
                  && stats.<Long>getLatestSampleValue(DeleteOutcome.NOT_FOUND.getStatisticName()).map(v -> v == 1).orElse(false)
                  && stats.<Long>getLatestSampleValue(StreamOutcome.SUCCESS.getStatisticName()).map(v -> v == 1).orElse(false));
        }
      }

      assertThat(collected).isTrue();
    }
  }

  @Test(timeout = 60_000)
  public void managementRegistryExposedOnAllServers() throws Exception {
    assumeFalse(datasetManagerType.isEmbedded());

    Cluster cluster = nmsService.readTopology();
    assertThat(cluster.serverStream()).hasSize(2);

    List<ServerEntity> entities = cluster.serverEntityStream()
        .filter(entity -> entity.getType().equals("com.terracottatech.store.client.DatasetEntity"))
        .filter(entity -> entity.getName().equals(getClass().getSimpleName() + "#" + testName.getMethodName()))
        .collect(Collectors.toList());
    assertThat(entities).hasSize(2);

    entities.forEach(serverEntity -> {
      assertThat(serverEntity.isManageable()).isTrue();
      List<String> names = serverEntity.getManagementRegistry().get().getCapabilities().stream().map(Capability::getName).collect(Collectors.toList());
      assertThat(names).contains("SovereignDatasetSettings", "SovereignDatasetStatistics", "SovereignIndexStatistics");
    });
  }

  @Test(timeout = 60_000)
  public void statisticsCollectedOnAllServers() throws Exception {
    assumeFalse(datasetManagerType.isEmbedded());

    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();

      Cluster cluster = nmsService.readTopology();

      List<ServerEntity> entities = cluster.serverEntityStream()
          .filter(entity -> entity.getType().equals("org.terracotta.management.entity.nms.client.NmsEntity"))
          .filter(ServerEntity::isManageable)
          .collect(Collectors.toList());
      assertThat(entities).hasSize(2);

      entities.forEach(entity -> {
        try {
          nmsService.startStatisticCollector(entity.getContext(), 2, TimeUnit.SECONDS).waitForReturn();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });

      int newId = Employees.generateUniqueEmpID();
      employeeWriterReader.add(newId, cellSet(20)); // add
      employeeWriterReader.add(newId, cellSet(20)); // duplicate
      employeeWriterReader.update(id(1), write(Employee.NAME).value("aaa")); // update
      employeeWriterReader.delete(id(0)); // delete not found

      Set<String> servers = new HashSet<>();
      while (servers.size() != 2 && !Thread.currentThread().isInterrupted()) {
        Message message = nmsService.waitForMessage();
        if (message.getType().equals("STATISTICS")) {
          message.unwrap(ContextualStatistics.class).stream()
              .peek(cs -> System.out.println(cs.getContext().get(Server.KEY) + " : " + cs.getStatistics().keySet()))
              .filter(stats -> stats.hasStatistic("SovereignDataset:AllocatedHeap") // all stats comes at the same time in the same object so we do not assert all their existence
                  && stats.hasStatistic("SovereignDataset:RecordCount")
                  && stats.hasStatistic("SovereignDataset:AllocatedPrimaryKey")
                  && stats.hasStatistic("SovereignDataset:OccupiedPrimaryKey"))
              .map(cs -> cs.getContext().get(Server.KEY))
              .forEach(servers::add);
        }
      }
      assertThat(servers).hasSize(2);
    }
  }

  @Test(timeout = 60_000)
  public void liveIndexCreation() throws Exception {
    assumeFalse(datasetManagerType.isEmbedded());

    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();

      assertThat(dataset.getIndexing().getAllIndexes()).isEmpty();
      assertThat(dataset.getIndexing().getLiveIndexes()).isEmpty();

      List<Settings> settings = nmsService.readTopology().serverEntityStream()
          .filter(entity -> entity.getType().equals("com.terracottatech.store.client.DatasetEntity"))
          .filter(entity -> entity.getName().equals(getClass().getSimpleName() + "#" + testName.getMethodName()))
          .map(entity -> entity.getManagementRegistry().get().getCapability("SovereignDatasetSettings").get())
          .flatMap(capability -> capability.getDescriptors(Settings.class).stream())
          .collect(Collectors.toList());

      assertThat(settings).hasSize(2); // active & passive
      settings.forEach(s -> assertThat(s.getStrings("runtimeIndexes")).isEmpty());

      int newId = Employees.generateUniqueEmpID();
      employeeWriterReader.add(newId, cellSet(20)); // add

      // clear previous messages
      nmsService.readMessages();

      dataset.getIndexing().createIndex(Employee.NAME, IndexSettings.BTREE).get(); // wait for index creation

      assertThat(dataset.getIndexing().getAllIndexes()).hasSize(1);
      assertThat(dataset.getIndexing().getLiveIndexes()).hasSize(1);

      // ensure notification has been sent from both active & passive
      long count = 0;
      while (count < 2) { // active & passive
        count += nmsService.readMessages()
            .stream()
            .filter(m -> m.getType().equals("NOTIFICATION"))
            .flatMap(m -> m.unwrap(ContextualNotification.class).stream())
            .filter(n -> n.getType().equals("DATASET_INDEX_CREATED"))
            .count();
        Thread.sleep(500);
      }

      // ensure we can see the new indexes in the topology
      settings = nmsService.readTopology().serverEntityStream()
          .filter(entity -> entity.getType().equals("com.terracottatech.store.client.DatasetEntity"))
          .filter(entity -> entity.getName().equals(getClass().getSimpleName() + "#" + testName.getMethodName()))
          .map(entity -> entity.getManagementRegistry().get().getCapability("SovereignDatasetSettings").get())
          .flatMap(capability -> capability.getDescriptors(Settings.class).stream())
          .collect(Collectors.toList());

      assertThat(settings).hasSize(2); // active & passive
      settings.forEach(s -> assertThat(s.getStrings("runtimeIndexes")).hasSize(1));
      settings.forEach(s -> assertThat(s.getStrings("runtimeIndexes")[0]).isEqualTo("name$$$String$$$BTREE"));
    }
  }

  @Before
  public void setupDatasetWriterReader() throws Exception {
    InternalDatasetManager internalDatasetManager = (InternalDatasetManager) datasetManagerType.getDatasetManager();
    statisticCollector = internalDatasetManager.getStatisticCollector();
    statisticCollector.stopStatisticCollector();

    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      initializeEmployeeList();

      // Insert only the 20 firsts to have some to fetch but also some still to add in my tests
      employeeList.stream()
          .sorted()
          .limit(20)
          .forEachOrdered(employee -> employeeWriterReader.add(employee.getEmpID(), employee.getCellSet()));
    }
    internalDatasetManager.setObserver(observer);
    localManagementRegistry = internalDatasetManager.getManagementRegistry();

    if (!datasetManagerType.isEmbedded()) {
      connectManagementClient(0);
    }
  }

  @After
  public void closeRegistry() throws Exception {
    if (managementConnection != null) {
      managementConnection.close();
    }
  }

  private Stream<? extends ContextualStatistics> queryAllStats() {
    String datasetManagerName = localManagementRegistry.getContextContainer().getValue();

    // get all possible stat names
    List<String> statNames = localManagementRegistry.getCapabilities()
        .stream()
        .filter(capability -> capability.getName().equals("DatasetStatistics"))
        .flatMap(capability -> capability.getDescriptors(StatisticDescriptor.class).stream())
        .map(StatisticDescriptor::getName)
        .collect(Collectors.toList());

    // get all cache contexts
    List<Context> contexts = localManagementRegistry.getCapabilities()
        .stream()
        .filter(capability -> capability.getName().equals("DatasetSettings"))
        .flatMap(capability -> capability.getDescriptors(Settings.class).stream())
        .filter(settings -> "Dataset".equals(settings.getString("type")))
        .map(descriptor -> Context.empty()
            .with("datasetManagerName", datasetManagerName)
            .with("datasetName", descriptor.getString("datasetName"))
            .with("datasetInstanceName", descriptor.getString("datasetInstanceName")))
        .collect(Collectors.toList());

    // do a management call to activate all stats from all contexts
    return localManagementRegistry.withCapability("DatasetStatistics")
        .queryStatistics(statNames)
        .on(contexts)
        .build()
        .execute()
        .results()
        .values()
        .stream();
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

    datasetManagerClient = () -> readTopology()
        .clientStream()
        .filter(cli -> cli.getName().equals("Store:" + datasetManagerType.name()))
        .reduce((result, client) -> {
          throw new IllegalStateException("DUPLICATE CLIENTS NAMED: " + "Store:" + datasetManagerType.name());
        })
        .get();
    remoteManagementRegistry = () -> datasetManagerClient.get().getManagementRegistry().get();
  }

  private Cluster readTopology() {
    try {
      return nmsService.readTopology();
    } catch (TimeoutException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      throw new RuntimeException(e.getCause());
    }
  }

  private JsonNode readJson(String file) {
    try {
      return objectMapper.readTree(new File(ManagementIT.class.getResource(file).toURI()));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private JsonNode toJson(Object o) {
    try {
      return objectMapper.readTree(objectMapper.writeValueAsString(o));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static abstract class CapabilityContextMixin {
    @JsonIgnore
    public abstract Collection<String> getRequiredAttributeNames();

    @JsonIgnore
    public abstract Collection<CapabilityContext.Attribute> getRequiredAttributes();
  }

}
