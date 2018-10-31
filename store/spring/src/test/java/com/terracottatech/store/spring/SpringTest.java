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
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.Type;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.manager.DatasetManager;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.net.URI;

import static com.terracottatech.store.Type.STRING;
import static com.terracottatech.store.manager.DatasetManager.clustered;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author Ludovic Orban
 */
public class SpringTest extends PassthroughTest {

  @Test
  public void springBare() throws Exception {
    ApplicationContext context = new ClassPathXmlApplicationContext("clustered-dataset-config.xml");
    try {
      @SuppressWarnings("unchecked")
      Dataset<String> dataset = context.getBean("personsDataset", Dataset.class);

      useDataset(dataset);
    } finally {
      ((ConfigurableApplicationContext) context).close();
    }
  }

  @Test
  public void springWithClusteredFactory() throws Exception {
    ApplicationContext context = new ClassPathXmlApplicationContext("clustered-dataset-config-withFactory.xml");
    try {
      @SuppressWarnings("unchecked")
      Dataset<String> dataset = context.getBean("personsDataset", Dataset.class);

      useDataset(dataset);
    } finally {
      ((ConfigurableApplicationContext) context).close();
    }
  }

  @Test
  public void springWithEmbeddedFactory() throws Exception {
    ApplicationContext context = new ClassPathXmlApplicationContext("embedded-dataset-config-withFactory.xml");
    try {
      @SuppressWarnings("unchecked")
      Dataset<String> dataset = context.getBean("personsDataset", Dataset.class);

      useDataset(dataset);
    } finally {
      ((ConfigurableApplicationContext) context).close();
    }
  }

  @Test
  public void code() throws Exception {
    try (DatasetManager datasetManager = clustered(URI.create("passthrough://stripe")).build()) {
      assertThat(datasetManager.newDataset("persons", STRING, datasetManager.datasetConfiguration()
          .offheap("offheap")
          .build()), is(true));
      try (Dataset<String> dataset = datasetManager.getDataset("persons", STRING)) {
        useDataset(dataset);
      }
    }
  }


  private void useDataset(Dataset<String> dataset) {
    DatasetWriterReader<String> rwAccess = dataset.writerReader();
    rwAccess.add("doe", CellDefinition.define("firstName", Type.STRING).newCell("john"));
  }

}
