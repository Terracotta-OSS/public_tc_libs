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
package com.terracottatech.store.systemtest;

import org.w3c.dom.Element;
import org.xmlunit.builder.Input;
import org.xmlunit.diff.DefaultNodeMatcher;
import org.xmlunit.diff.ElementSelectors;

import java.net.URL;

import static org.junit.Assert.assertThat;
import static org.xmlunit.matchers.CompareMatcher.isSimilarTo;

/**
 * TranslateTestHelper
 */
public class TranslateTestHelper {
  public static void assertElementWithFilter(URL input, String config) {
    assertThat(Input.from(config), isSimilarTo(Input.fromURL(input)).ignoreComments()
        .ignoreWhitespace()
        .withNodeFilter(n -> !(n instanceof Element && "client-alias".equals(n.getLocalName())))
        .withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndAllAttributes)));
  }

  public static void assertElementXml(URL input, String config) {
    assertThat(Input.from(config), isSimilarTo(Input.fromURL(input)).ignoreComments()
        .ignoreWhitespace()
        .withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndAllAttributes)));

  }
}
