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
package com.terracottatech.tools.clustertool.result;

public class ClusterToolCommandResults {
  // Message strings to ensure consistency across different commands
  public static final String COMMAND_FAILURE_MESSAGE = "Command failed.";
  public static final String COMMAND_PARTIAL_FAILURE_MESSAGE = "Command completed with errors.";
  public static final String COMMAND_SUCCESS_MESSAGE = "Command completed successfully.";
  public static final String LICENSE_INSTALLATION_MESSAGE = "License installation successful";
  public static final String LICENSE_NOT_UPDATED_MESSAGE = "License not updated (Reason: Identical to previously installed license)";
  public static final String CONFIGURATION_UPDATE_MESSAGE = "Configuration successful";

  // Exit status codes, loosely modeled on http status codes.
  public enum StatusCode {
    SUCCESS(0),
    PARTIAL_FAILURE(1),
    FAILURE(2),
    ALREADY_CONFIGURED(28),
    BAD_REQUEST(40),
    NOT_FOUND(44),
    HANDSHAKE_OR_SECURITY_ERROR(48),
    CONFLICT(49),
    INTERNAL_ERROR(50),
    NOT_IMPLEMENTED(51),
    BAD_GATEWAY(52);

    private int code;

    StatusCode(int code) {
      this.code = code;
    }

    public int getCode() {
      return code;
    }

    public String toString() {
      return String.format("%s(%d)", name(), getCode());
    }
  }

  private ClusterToolCommandResults() {
    // Prevent instantiation since this class is supposed to be used for constants only
  }
}
