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
package com.terracottatech.store.server.execution;

import com.tc.classloader.BuiltinService;
import org.terracotta.entity.PlatformConfiguration;
import org.terracotta.entity.ServiceConfiguration;
import org.terracotta.entity.ServiceProvider;
import org.terracotta.entity.ServiceProviderCleanupException;
import org.terracotta.entity.ServiceProviderConfiguration;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

@BuiltinService
public class ExecutionServiceProvider implements ServiceProvider {

  private static final ThreadGroup THREAD_GROUP = new ThreadGroup(Thread.currentThread().getThreadGroup(),"TCStore Default Workers");

  private static final ThreadFactory DEFAULT_THREAD_FACTORY = new ThreadFactory() {
    private final AtomicInteger threadCount = new AtomicInteger();

    @Override
    public Thread newThread(Runnable r) {
      return new Thread(THREAD_GROUP, r, "Unordered:default:" + threadCount.getAndIncrement());
    }
  };

  private ExecutionService executionService;

  @Override
  public boolean initialize(ServiceProviderConfiguration serviceProviderConfiguration, PlatformConfiguration platformConfiguration) {
    ThreadPoolExecutor defaultPool = new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors(), Runtime.getRuntime().availableProcessors(),
        30L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
        DEFAULT_THREAD_FACTORY
    );
    defaultPool.allowCoreThreadTimeOut(true);
    executionService = new ExecutionServiceImpl(
        Arrays.asList(new AbstractMap.SimpleImmutableEntry<>(Pattern.compile(".*"), defaultPool))
    );
    return true;
  }

  @Override
  public <T> T getService(long l, ServiceConfiguration<T> serviceConfiguration) {
    return serviceConfiguration.getServiceType().cast(executionService);
  }

  @Override
  public Collection<Class<?>> getProvidedServiceTypes() {
    return Collections.singleton(ExecutionService.class);
  }

  @Override
  public void prepareForSynchronization() throws ServiceProviderCleanupException {
  }
}
