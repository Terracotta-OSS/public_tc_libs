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
package com.terracottatech.connection;

import com.tc.net.protocol.transport.TransportHandshakeException;
import com.terracotta.connection.api.DetailedConnectionException;
import java.io.EOFException;
import org.terracotta.connection.ConnectionException;

import java.util.List;
import java.util.Map;
import java.util.Properties;

class ExceptionUtils {
  static ConnectionException getConnectionException(DetailedConnectionException e, Properties properties) {
    Map<String, List<Exception>> connectionErrorMap = e.getConnectionErrorMap();
    boolean handshakeExceptionFound = false;
    if (connectionErrorMap != null) {
      handshakeExceptionFound = connectionErrorMap.values().stream().anyMatch(ExceptionUtils::containsTransportOrEOFException);
    }

    if (handshakeExceptionFound) {
      return new ConnectionException(new HandshakeOrSecurityException(getErrorMessage(properties), e));
    }
    return e;
  }

  private static boolean containsTransportHandshakeException(List<Exception> throwableList) {
    return TransportHandshakeException.class.equals(throwableList.get(throwableList.size() - 1).getClass());
  }

  private static boolean containsEOFException(List<Exception> throwableList) {
    return throwableList.get(throwableList.size() - 1) instanceof EOFException;
  }

  private static boolean containsTransportOrEOFException(List<Exception> throwableList) {
    return containsEOFException(throwableList) || containsTransportHandshakeException(throwableList);
  }

  private static String getErrorMessage(Properties properties) {
    String errMsg;
    if (properties.getProperty(EnterpriseConnectionPropertyNames.SECURITY_ROOT_DIRECTORY) != null) {
      errMsg = "Handshake with server failed when this client tried to initiate a secure connection. This can happen, " +
               "for example, when the client's security configuration does not match the server's, the server is stuck " +
               "in a split-brain, or the server is shutting down. Check client and server logs for more information";
    } else {
      errMsg = "Handshake with server failed when this client tried to initiate a non-secure connection. This can happen, " +
               "for example, when the server is running with security, is stuck in a split-brain, or is shutting down. " +
               "Check client and server logs for more information";
    }
    return errMsg;
  }
}
