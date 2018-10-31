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
package com.terracottatech.tools.clustertool.exceptions;

import com.terracottatech.tools.clustertool.result.ClusterToolCommandResults.StatusCode;

/**
 * Convenience exception that wraps a status code that can serve as an exit code.
 */
public class ClusterToolException extends RuntimeException {

  private static final long serialVersionUID = -4486628717440257533L;

  private final StatusCode statusCode;

  public ClusterToolException(StatusCode statusCode, String message) {
    super(message);
    this.statusCode = statusCode;
  }

  public ClusterToolException(StatusCode statusCode, Throwable cause) {
    super(cause);
    this.statusCode = statusCode;
  }

  public ClusterToolException(StatusCode statusCode, String message, Throwable cause) {
    super(message, cause);
    this.statusCode = statusCode;
  }

  public StatusCode getStatusCode() {
    return statusCode;
  }
}
