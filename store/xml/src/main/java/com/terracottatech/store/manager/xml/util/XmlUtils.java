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
package com.terracottatech.store.manager.xml.util;

import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.SchemaFactory;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;

public class XmlUtils {
  public static String getAttribute(Node node, String attributeName) {
    return ((Element)node).getAttributeNode(attributeName).getValue();
  }

  public static Optional<String> getOptionalAttribute(Node node, String attributeName) {
    Attr attribute = ((Element)node).getAttributeNode(attributeName);
    return Optional.ofNullable(attribute).map(Attr::getValue);
  }

  public static String getNodeValue(Node node) {
    return node.getFirstChild().getNodeValue();
  }

  public static Document getValidatedDocument(InputStream inputStream, List<URL> schemas) throws Exception {
    DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
    documentBuilderFactory.setNamespaceAware(true);
    documentBuilderFactory.setIgnoringComments(true);
    documentBuilderFactory.setIgnoringElementContentWhitespace(true);

    SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
    StreamSource[] streamSources = new StreamSource[schemas.size() + 1];  // +1 for that base dataset schema
    streamSources[0] = new StreamSource(XmlUtils.class.getResourceAsStream("/dataset.xsd"));
    for (int i = 0; i < schemas.size(); i++) {
      streamSources[i+1] = new StreamSource(schemas.get(i).openStream());
    }
    documentBuilderFactory.setSchema(schemaFactory.newSchema(streamSources));

    DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
    CollectingErrorHandler errorHandler = new CollectingErrorHandler();
    documentBuilder.setErrorHandler(errorHandler);

    Document document = documentBuilder.parse(inputStream);

    List<SAXParseException> errors = errorHandler.getErrors();
    if(!errors.isEmpty()) {
      SAXException firstException = errors.get(0);
      for (int i = 1; i < errors.size(); i++) {
        firstException.addSuppressed(errors.get(i));
      }
      throw firstException;
    }

    return document;
  }

  public static String convertToString(Document document) throws Exception {
    TransformerFactory transformerFactory = TransformerFactory.newInstance();
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    transformerFactory.newTransformer()
                      .transform(new DOMSource(document), new StreamResult(byteArrayOutputStream));
    return new String(byteArrayOutputStream.toByteArray(), UTF_8);
  }

  private static class CollectingErrorHandler implements ErrorHandler {

    private final List<SAXParseException> errors = new ArrayList<>();

    @Override
    public void error(SAXParseException exception) {
      errors.add(exception);
    }

    @Override
    public void fatalError(SAXParseException exception) {
      errors.add(exception);
    }

    @Override
    public void warning(SAXParseException exception) {
    }

    List<SAXParseException> getErrors() {
      return Collections.unmodifiableList(errors);
    }
  }
}
