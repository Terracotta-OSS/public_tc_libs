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
package com.terracottatech.store.spring;

import com.terracottatech.store.Dataset;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.store.StoreException;
import com.terracottatech.store.Type;
import com.terracottatech.store.configuration.DatasetConfigurationBuilder;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;

/**
 * @author Ludovic Orban
 */
public class DatasetFactoryBean<T extends Comparable<T>> implements InitializingBean, DisposableBean, FactoryBean<Dataset<T>> {

  private final Config<T> config = new Config<>();

  private DatasetManager datasetManager;
  private Dataset<T> dataset;

  @Override
  public Dataset<T> getObject() throws Exception {
    return dataset;
  }

  @Override
  public Class<?> getObjectType() {
    return Dataset.class;
  }

  @Override
  public boolean isSingleton() {
    return true;
  }

  @Override
  public void afterPropertiesSet() throws Exception {
    init();
  }

  private void init() throws StoreException {
    DatasetConfigurationBuilder configurationBuilder = datasetManager.datasetConfiguration();
    if (config.offheapResourceName != null) {
      configurationBuilder = configurationBuilder.offheap(config.offheapResourceName);
    }
    if (config.diskResourceName != null) {
      configurationBuilder = configurationBuilder.disk(config.diskResourceName);
    }
    datasetManager.newDataset(config.name, config.type, configurationBuilder.build());
    this.dataset = datasetManager.getDataset(config.name, config.type);
  }

  @Override
  public void destroy() throws Exception {
    if (dataset != null) {
      dataset.close();
    }
  }

  public DatasetManager getDatasetManager() {
    return datasetManager;
  }

  public void setDatasetManager(DatasetManager datasetManager) {
    this.datasetManager = datasetManager;
  }

  public Config<T> getConfig() {
    return config;
  }

  public static class Config<T extends Comparable<T>> {
    private String name;
    private Type<T> type;
    private String offheapResourceName;
    private String diskResourceName;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public Type<T> getType() {
      return type;
    }

    public void setType(Type<T> type) {
      this.type = type;
    }

    public String getOffheapResourceName() {
      return offheapResourceName;
    }

    public void setOffheapResourceName(String offheapResourceName) {
      this.offheapResourceName = offheapResourceName;
    }

    public String getDiskResourceName() {
      return diskResourceName;
    }

    public void setDiskResourceName(String diskResourceName) {
      this.diskResourceName = diskResourceName;
    }

  }

}
