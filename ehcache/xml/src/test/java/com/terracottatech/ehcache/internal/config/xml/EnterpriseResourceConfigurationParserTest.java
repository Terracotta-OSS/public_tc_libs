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

package com.terracottatech.ehcache.internal.config.xml;

import org.ehcache.clustered.client.internal.config.DedicatedClusteredResourcePoolImpl;
import org.ehcache.clustered.client.internal.config.SharedClusteredResourcePoolImpl;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.internal.util.ClassLoading;
import org.ehcache.xml.CacheResourceConfigurationParser;
import org.junit.Test;
import org.w3c.dom.Attr;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import com.terracottatech.ehcache.clustered.client.internal.config.EnterpriseClusteredDedicatedResourcePoolImpl;
import com.terracottatech.ehcache.clustered.client.internal.config.EnterpriseClusteredSharedResourcePoolImpl;
import com.terracottatech.ehcache.disk.internal.config.FastRestartStoreResourcePoolImpl;

import java.util.ServiceLoader;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.stream.StreamSource;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static com.terracottatech.ehcache.internal.config.xml.EnterpriseResourceConfigurationParserTestHelper.assertElement;

public class EnterpriseResourceConfigurationParserTest {

  @Test
  public void testServiceLocatable() {
    String expectedParser = EnterpriseDiskResourceConfigurationParser.class.getName();
    final ServiceLoader<CacheResourceConfigurationParser> parsers =
        ClassLoading.libraryServiceLoaderFor(CacheResourceConfigurationParser.class);
    boolean found = false;
    for (final CacheResourceConfigurationParser parser : parsers) {
      if (parser.getClass().getName().equals(expectedParser)) {
        found = true;
        break;
      }
    }
    if (!found) {
      fail("Expected Parser Not Found");
    }
  }

  @Test
  public void testTargetNameSpace() throws Exception {
    EnterpriseDiskResourceConfigurationParser parserUnderTest = new EnterpriseDiskResourceConfigurationParser();
    StreamSource schemaSource = (StreamSource) parserUnderTest.getXmlSchema();

    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();

    DocumentBuilder domBuilder = factory.newDocumentBuilder();
    Element schema = domBuilder.parse(schemaSource.getInputStream()).getDocumentElement();
    Attr targetNamespaceAttr = schema.getAttributeNode("targetNamespace");
    assertThat(targetNamespaceAttr, is(not(nullValue())));
    assertThat(targetNamespaceAttr.getValue(), is(parserUnderTest.getNamespace().toString()));
  }

  @Test
  public void testTranslationFromXmlToConfigForDiskParser() {
    FastRestartStoreResourcePoolImpl fastRestartStoreResourcePool = new FastRestartStoreResourcePoolImpl(12, MemoryUnit.GB);
    EnterpriseDiskResourceConfigurationParser parser = new EnterpriseDiskResourceConfigurationParser();
    Node rootElement = parser.unparseResourcePool(fastRestartStoreResourcePool);
    String inputString = "<tc:disk-restartable unit=\"GB\" xmlns:tc=\"http://www.terracottatech.com/v3/terracotta/ehcache\">12</tc:disk-restartable>";
    assertElement(inputString, rootElement);
  }

  @Test
  public void testTranslationFromXmlToConfigForEnterpriseClusteredShared() {
    EnterpriseClusteredSharedResourcePoolImpl enterpriseClusteredSharedResourcePool =
        new EnterpriseClusteredSharedResourcePoolImpl("my-shared");
    EnterpriseClusteredResourceConfigurationParser parser = new EnterpriseClusteredResourceConfigurationParser();
    Node rootElement = parser.unparseResourcePool(enterpriseClusteredSharedResourcePool);
    String inputString = "<tc:clustered-restartable-shared sharing=\"my-shared\" " +
                         "xmlns:tc=\"http://www.terracottatech.com/v3/terracotta/ehcache\"></tc:clustered-restartable-shared>";
    assertElement(inputString, rootElement);
  }

  @Test
  public void testTranslationFromXmlToConfigForEnterpriseClusteredDedicatedWithDataPercent() {
    EnterpriseClusteredDedicatedResourcePoolImpl enterpriseClusteredDedicatedResourcePool =
        new EnterpriseClusteredDedicatedResourcePoolImpl("my-resource", 12, MemoryUnit.GB, 25);
    EnterpriseClusteredResourceConfigurationParser parser = new EnterpriseClusteredResourceConfigurationParser();
    Node rootElement = parser.unparseResourcePool(enterpriseClusteredDedicatedResourcePool);
    String inputString = "<tc:clustered-restartable-dedicated from=\"my-resource\" " +
                         "unit=\"GB\" data-percent = \"25\" " +
                         "xmlns:tc=\"http://www.terracottatech.com/v3/terracotta/ehcache\">12</tc:clustered-restartable-dedicated>";
    assertElement(inputString, rootElement);
  }

  @Test
  public void testTranslationFromXmlToConfigForEnterpriseClusteredDedicated() {
    EnterpriseClusteredDedicatedResourcePoolImpl enterpriseClusteredDedicatedResourcePool =
        new EnterpriseClusteredDedicatedResourcePoolImpl("my-resource", 12, MemoryUnit.GB);
    EnterpriseClusteredResourceConfigurationParser parser = new EnterpriseClusteredResourceConfigurationParser();
    Node rootElement = parser.unparseResourcePool(enterpriseClusteredDedicatedResourcePool);
    String inputString = "<tc:clustered-restartable-dedicated from=\"my-resource\" unit=\"GB\" " +
                         "xmlns:tc=\"http://www.terracottatech.com/v3/terracotta/ehcache\">12</tc:clustered-restartable-dedicated>";
    assertElement(inputString, rootElement);
  }

  @Test
  public void testTranslationFromXmlToConfigForCoreClusterShared() {
    SharedClusteredResourcePoolImpl sharedClusteredResourcePool = new SharedClusteredResourcePoolImpl("my-sharing");
    EnterpriseClusteredResourceConfigurationParser parser = new EnterpriseClusteredResourceConfigurationParser();
    Node rootElement = parser.unparseResourcePool(sharedClusteredResourcePool);
    String inputString = "<tc:clustered-shared sharing=\"my-sharing\"  " +
                         "xmlns:tc=\"http://www.terracottatech.com/v3/terracotta/ehcache\"></tc:clustered-shared>";
    assertElement(inputString, rootElement);
  }

  @Test
  public void testTranslationFromXmlToConfigForCoreClusterDedicated() {
    DedicatedClusteredResourcePoolImpl dedicatedClusteredResourcePool =
        new DedicatedClusteredResourcePoolImpl("my-resource", 12, MemoryUnit.GB);
    EnterpriseClusteredResourceConfigurationParser parser = new EnterpriseClusteredResourceConfigurationParser();
    Node rootElement = parser.unparseResourcePool(dedicatedClusteredResourcePool);
    String inputString = "<tc:clustered-dedicated from=\"my-resource\" unit=\"GB\" " +
                         "xmlns:tc=\"http://www.terracottatech.com/v3/terracotta/ehcache\">12</tc:clustered-dedicated>";
    assertElement(inputString, rootElement);
  }
}