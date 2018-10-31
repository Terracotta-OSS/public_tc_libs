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
import com.terracottatech.store.manager.EmbeddedDatasetManagerBuilder;
import com.terracottatech.store.manager.EmbeddedDatasetManagerBuilder.FileMode;
import com.terracottatech.store.manager.EmbeddedDatasetManagerBuilder.PersistenceMode;
import com.terracottatech.store.configuration.MemoryUnit;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;

import java.nio.file.Path;

import static com.terracottatech.store.manager.DatasetManager.embedded;

public class EmbeddedDatasetManagerFactoryBean implements InitializingBean, DisposableBean, FactoryBean<DatasetManager> {

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
    EmbeddedDatasetManagerBuilder builder = embedded();

    if (config.offheapName == null) {
      throw new StoreException("Please specify an offheapName");
    }

    if (config.offheapSize == null) {
      throw new StoreException("Please specify an offheapSize");
    }

    builder = builder.offheap(config.offheapName, config.offheapSize, MemoryUnit.B);

    if (config.diskPath != null) {
      builder = builder.disk(config.diskName, config.diskPath, config.persistenceMode, config.fileMode);
    }

    this.datasetManager = builder.build();
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
    private String offheapName;
    private Long offheapSize;
    private String diskName;
    private Path diskPath;
    private FileMode fileMode = FileMode.REOPEN_OR_NEW;
    private PersistenceMode persistenceMode = PersistenceMode.INMEMORY;

    public String getOffheapName() {
      return offheapName;
    }

    public void setOffheapName(String offheapName) {
      this.offheapName = offheapName;
    }

    public Long getOffheapSize() {
      return offheapSize;
    }

    public void setOffheapSize(Long offheapSize) {
      this.offheapSize = offheapSize;
    }

    public String getDiskName() {
      return diskName;
    }

    public void setDiskName(String diskName) {
      this.diskName = diskName;
    }

    public Path getDiskPath() {
      return diskPath;
    }

    public void setDiskPath(Path diskPath) {
      this.diskPath = diskPath;
    }

    public FileMode getFileMode() {
      return fileMode;
    }

    public void setFileMode(FileMode fileMode) {
      this.fileMode = fileMode;
    }

    public PersistenceMode getPersistenceMode() {
      return persistenceMode;
    }

    public void setPersistenceMode(PersistenceMode persistenceMode) {
      this.persistenceMode = persistenceMode;
    }
  }
}
