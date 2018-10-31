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
package com.terracottatech.store.management;

import com.terracottatech.store.StoreException;
import com.terracottatech.store.Type;
import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.configuration.DatasetConfigurationBuilder;
import com.terracottatech.store.internal.InternalDatasetManager;
import com.terracottatech.store.manager.DatasetManagerConfiguration;
import com.terracottatech.store.statistics.StatisticsDataset;
import com.terracottatech.store.statistics.StatisticsDatasetManager;
import com.terracottatech.store.statistics.StatisticsService;
import com.terracottatech.store.util.Exceptions;
import com.terracottatech.store.util.ExecutorUtil;
import org.terracotta.management.model.context.Context;
import org.terracotta.management.model.context.ContextContainer;
import org.terracotta.management.model.notification.ContextualNotification;
import org.terracotta.management.registry.DefaultManagementRegistry;
import org.terracotta.management.registry.ManagementRegistry;
import org.terracotta.management.registry.collect.DefaultStatisticCollector;
import org.terracotta.management.registry.collect.StatisticCollector;
import org.terracotta.statistics.Time;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

public class ManageableDatasetManager implements InternalDatasetManager {

  static final String KEY_DM_NAME = "datasetManagerName";
  static final String KEY_D_NAME = "datasetName";
  static final String KEY_D_INST = "datasetInstanceName";

  private final DefaultManagementRegistry managementRegistry;
  private final String datasetManagerAlias;
  private final StatisticsDatasetManager underlying;
  private final Context parentContext;
  private final StatisticCollector statisticCollector;
  private final ThreadGroup threadGroup;
  private final AtomicInteger threadNumber = new AtomicInteger(1);
  private final ScheduledExecutorService scheduler;

  private volatile ManageableObserver observer = ManageableObserver.NOOP;

  public ManageableDatasetManager(String datasetManagerAlias, String instanceId, StatisticsDatasetManager underlying) {
    if (instanceId == null) {
      instanceId = newShortUUID();
    }

    this.datasetManagerAlias = datasetManagerAlias == null ? newShortUUID() : datasetManagerAlias;
    this.underlying = Objects.requireNonNull(underlying);
    this.threadGroup = new ThreadGroup(datasetManagerAlias);

    this.scheduler = new ScheduledThreadPoolExecutor(1, ManageableDatasetManager.this::createThread, new ThreadPoolExecutor.AbortPolicy());

    this.managementRegistry = new DefaultManagementRegistry(new ContextContainer(KEY_DM_NAME, this.datasetManagerAlias, new ArrayList<>())) {
      @Override
      public ContextContainer getContextContainer() {
        return new ContextContainer(
            KEY_DM_NAME, getDatasetManagerAlias(),
            underlying.getStatisticsService().getDatasetStatistics()
                .stream()
                .map(ds -> new ContextContainer(KEY_D_INST, ds.getInstanceName()))
                .collect(Collectors.toList()));
      }
    };

    // routing context that identifies managed objects
    this.parentContext = Context.create(KEY_DM_NAME, datasetManagerAlias);
    // expose dataset settings (names and instance names)
    this.managementRegistry.addManagementProvider(new DatasetSettingsManagementProvider(instanceId));
    // expose dataset statistics
    this.managementRegistry.addManagementProvider(new DatasetStatisticsManagementProvider(parentContext, getTimeSource()));
    // handle management call on objects of type StatisticCollector to start/stop collecting statistics
    this.managementRegistry.addManagementProvider(new DatasetStatisticCollectorProvider(parentContext));

    // creates a collector
    this.statisticCollector = new DefaultStatisticCollector(
        managementRegistry,
        scheduler,
        statistics -> getObserver().onStatistics(statistics),
        getTimeSource());

    managementRegistry.register(statisticCollector);
  }

  @Override
  public String getDatasetManagerAlias() {
    return datasetManagerAlias;
  }

  @Override
  public ManagementRegistry getManagementRegistry() {
    return managementRegistry;
  }

  @Override
  public StatisticCollector getStatisticCollector() {
    return statisticCollector;
  }

  @Override
  public void setObserver(ManageableObserver observer) {
    this.observer = Objects.requireNonNull(observer);
  }

  @Override
  public StatisticsService getStatisticsService() {
    return underlying.getStatisticsService();
  }

  @Override
  public Thread createThread(Runnable r) {
    return new Thread(threadGroup, r, getDatasetManagerAlias() + "-thread-" + threadNumber.getAndIncrement());
  }

  @Override
  public LongSupplier getTimeSource() {
    return Time::absoluteTime;
  }

  @Override
  public DatasetManagerConfiguration getDatasetManagerConfiguration() {
    return underlying.getDatasetManagerConfiguration();
  }

  @Override
  public <K extends Comparable<K>> boolean newDataset(String name, Type<K> keyType, DatasetConfiguration configuration) throws StoreException {
    return underlying.newDataset(name, keyType, configuration);
  }

  @Override
  public <K extends Comparable<K>> ManageableDataset<K> getDataset(String name, Type<K> keyType) throws StoreException {
    return wrap(underlying.getDataset(name, keyType));
  }

  @Override
  public Map<String, Type<?>> listDatasets() throws StoreException {
    return underlying.listDatasets();
  }

  @Override
  public boolean destroyDataset(String name) throws StoreException {
    return underlying.destroyDataset(name);
  }

  @Override
  public void close() {
    Exceptions.suppress(() -> ExecutorUtil.shutdown(scheduler), managementRegistry::close, underlying::close);
  }

  @Override
  public DatasetConfigurationBuilder datasetConfiguration() {
    return underlying.datasetConfiguration();
  }

  public Context getParentContext() {
    return parentContext;
  }

  protected ManageableObserver getObserver() {
    return observer;
  }

  private <K extends Comparable<K>> ManageableDataset<K> wrap(StatisticsDataset<K> dataset) {
    ManageableDataset<K> manageableDataset = new ManageableDataset<>(parentContext, dataset, me -> {
      try {
        getObserver().onNotification(new ContextualNotification(me.getContext(), Notification.DATASET_INSTANCE_CLOSED.name()));
      } finally {
        getManagementRegistry().unregister(me);
      }
    });
    getManagementRegistry().register(manageableDataset);
    getObserver().onNotification(new ContextualNotification(manageableDataset.getContext(), Notification.DATASET_INSTANCE_CREATED.name()));
    return manageableDataset;
  }

  /**
   * generate a shorter UUID string
   * In example aa6cdec3-2026-470e-a0ff-c45d57abbdf7 will give DkcmIMPebKr3vatXXcT_oA instead
   */
  public static String newShortUUID() {
    UUID j = UUID.randomUUID();
    byte[] data = new byte[16];
    long msb = j.getMostSignificantBits();
    long lsb = j.getLeastSignificantBits();
    for (int i = 0; i < 8; i++) {
      data[i] = (byte) (msb & 0xff);
      msb >>>= 8;
    }
    for (int i = 8; i < 16; i++) {
      data[i] = (byte) (lsb & 0xff);
      lsb >>>= 8;
    }
    return Base64.getUrlEncoder().encodeToString(data).replace("=", "");
  }

}
