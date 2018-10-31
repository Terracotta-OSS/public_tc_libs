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
package com.terracottatech.store.systemtest.osgi;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerMethod;

import com.terracottatech.store.StoreException;
import com.terracottatech.store.manager.ClusteredDatasetManagerBuilder;
import com.terracottatech.store.manager.DatasetManager;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import static org.ops4j.pax.exam.CoreOptions.bundle;
import static org.ops4j.pax.exam.CoreOptions.junitBundles;
import static org.ops4j.pax.exam.CoreOptions.mavenBundle;
import static org.ops4j.pax.exam.CoreOptions.options;

/**
 * BasicOsgiTest
 */
@Ignore("because we have a bug that will be fixed in a later platform version (16)")
@RunWith(PaxExam.class)
@ExamReactorStrategy(PerMethod.class)
public class BasicOsgiTest {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Configuration
  public Option[] config() {
    return options(
        mavenBundle("org.slf4j", "slf4j-api", System.getProperty("osgi.slf4j.version")),
        mavenBundle("ch.qos.logback", "logback-core", System.getProperty("osgi.logback.version")),
        mavenBundle("ch.qos.logback", "logback-classic", System.getProperty("osgi.logback.version")),
        bundle("file:" + System.getProperty("common.osgi.jar")),
        bundle("file:" + System.getProperty("ehcache.osgi.jar")),
        bundle("file:" + System.getProperty("store.osgi.jar")),
        junitBundles()
    );
  }

  @Test
  public void testVeryBasicTCStoreAsBundle() throws Exception {
    expectedException.expect(StoreException.class);
    expectedException.expectMessage("TimeoutException");

    ClusteredDatasetManagerBuilder builder = DatasetManager.clustered(URI.create("terracotta://localhost:9510"))
        .withConnectionTimeout(1, TimeUnit.SECONDS);
    builder.build();
  }
}
