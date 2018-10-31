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

import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.store.StoreException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;

import java.net.URI;

import static com.terracottatech.store.manager.DatasetManager.clustered;

public class ClusteredDatasetManagerFactoryBean implements InitializingBean, DisposableBean, FactoryBean<DatasetManager> {

  private final Config config = new Config();

  private DatasetManager datasetManager;

  @Override
  public DatasetManager getObject() throws Exception {
    return datasetManager;
  }

  @Override
  public Class<?> getObjectType() {
    return DatasetManager.class;
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
    this.datasetManager = clustered(this.config.clusterURI).build();
  }

  @Override
  public void destroy() throws Exception {
    if (datasetManager != null) {
      datasetManager.close();
    }
  }

  public Config getConfig() {
    return config;
  }

  public static class Config {

    private URI clusterURI;

    public URI getClusterURI() {
      return clusterURI;
    }

    public void setClusterURI(URI clusterURI) {
      this.clusterURI = clusterURI;
    }

  }

}
