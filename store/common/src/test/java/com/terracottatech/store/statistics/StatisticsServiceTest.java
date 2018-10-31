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
package com.terracottatech.store.statistics;

import org.junit.Test;

import com.terracottatech.store.Dataset;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.mock;

/**
 * @author Henri Tremblay
 */
public class StatisticsServiceTest {

  private StatisticsService statisticsService = new StatisticsService();
  @SuppressWarnings("unchecked")
  private Dataset<String> dataset = mock(Dataset.class);

  @Test
  public void getDatasets() throws Exception {
    DatasetStatistics stats = statisticsService.registerDataset("employee", dataset);
    String name = stats.getInstanceName();
    assertThat(statisticsService.getDatasetStatistics()).first().matches(d -> d.getInstanceName().equals(name));
    statisticsService.unregisterDataset(name );
    assertThat(statisticsService.getDatasetStatistics()).isEmpty();
    assertThat(statisticsService.getDatasetInstanceNames()).isEmpty();
  }

  @Test
  public void registerUnregister() throws Exception {
    DatasetStatistics stats = statisticsService.registerDataset("employee", dataset);
    String name = stats.getInstanceName();
    statisticsService.getDatasetInstanceNames().stream()
        .forEach(n -> assertThat(statisticsService.get(n)).isNotNull());
    statisticsService.unregisterDataset(name);
    assertThat(statisticsService.get(name)).isNull();
  }

}
