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
package com.terracottatech.store.client.management;

import com.terracottatech.store.client.InternalClusteredDatasetManager;
import com.terracottatech.store.client.reconnectable.ReconnectableLeasedConnection;
import com.terracottatech.store.management.ManageableDatasetManager;
import com.terracottatech.store.management.ManageableObserver;
import com.terracottatech.store.management.Notification;
import com.terracottatech.store.statistics.StatisticsDatasetManager;
import com.terracottatech.store.util.Exceptions;
import com.terracottatech.store.util.ExecutorUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.connection.Connection;
import org.terracotta.management.entity.nms.agent.client.DefaultNmsAgentService;
import org.terracotta.management.entity.nms.agent.client.NmsAgentEntityFactory;
import org.terracotta.management.entity.nms.agent.client.NmsAgentService;
import org.terracotta.management.model.notification.ContextualNotification;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ClusteredManageableDatasetManager extends ManageableDatasetManager implements InternalClusteredDatasetManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusteredManageableDatasetManager.class);
  private static final Logger LOGGER_CALL = LoggerFactory.getLogger(ClusteredManageableDatasetManager.class + ".managementCallExecutor");
  private static final int DEFAULT_NMS_OPS_TIMEOUT_SEC = 5;

  private final Connection connection;
  private final ExecutorService executorService;
  private final NmsAgentService nmsAgentService;

  private volatile boolean closed;

  public ClusteredManageableDatasetManager(String datasetManagerName, String instanceId, Set<String> tags, StatisticsDatasetManager underlying, Connection connection) {
    super(datasetManagerName, instanceId, underlying);
    this.connection = Objects.requireNonNull(connection);
    this.executorService = new ThreadPoolExecutor(
        1, 1,
        0L, TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<>(1024),
        this::createThread,
        (task, executor) -> {
          if (!executor.isShutdown()) {
            LOGGER.error("Management Call queue is full for dataset manager " + getDatasetManagerAlias());
          }
        });

    this.nmsAgentService = createNmsAgentService();
    this.setObserver(ManageableObserver.NOOP);

    nmsAgentService.sendStates();
    nmsAgentService.setTags(tags);
  }

  @Override
  public Connection getConnection() {
    return connection;
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      if (!nmsAgentService.isDisconnected()) {
        getObserver().onNotification(new ContextualNotification(getParentContext(), Notification.DATASET_MANAGER_CLOSED.name()));
      }
      Exceptions.suppress(() -> ExecutorUtil.shutdown(executorService), nmsAgentService::close, super::close);
    }
  }

  @Override
  public void setObserver(ManageableObserver observer) {
    super.setObserver(new InterceptingManageableObserver(observer, nmsAgentService));
  }

  private NmsAgentService createNmsAgentService() {
    DefaultNmsAgentService nmsAgentService = new DefaultNmsAgentService(() -> new NmsAgentEntityFactory(getConnection()).retrieve());

    nmsAgentService.setOperationTimeout(DEFAULT_NMS_OPS_TIMEOUT_SEC, TimeUnit.SECONDS);
    nmsAgentService.setManagementRegistry(getManagementRegistry());

    // we cannot execute anything inside the voltron callback thread
    nmsAgentService.setManagementCallExecutor(command -> executorService.execute(() -> {
      if (!closed) {
        try {
          command.run();
        } catch (RuntimeException e) {
          LOGGER_CALL.error("Error executing management call on dataset manager '" + getDatasetManagerAlias() + "' : " + e.getMessage(), e);
        }
      }
    }));

    if (connection instanceof ReconnectableLeasedConnection) {
      ((ReconnectableLeasedConnection) connection).setReconnectListener(() -> {
        nmsAgentService.flushEntity();
        nmsAgentService.sendStates();
      });
    }

    return nmsAgentService;
  }

}
