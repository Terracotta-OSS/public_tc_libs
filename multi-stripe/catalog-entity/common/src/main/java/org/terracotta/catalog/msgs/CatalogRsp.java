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
package org.terracotta.catalog.msgs;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.nio.ByteBuffer;
import org.terracotta.entity.EntityResponse;

/**
 *
 */
public class CatalogRsp implements EntityResponse {

  private final boolean result;
  private final byte[] payload;

  public CatalogRsp(boolean base, ByteBuffer payload) {
    this.result = base;
    if (payload != null) {
      byte[] data = new byte[payload.remaining()];
      payload.get(data);
      this.payload = data;
    } else {
      this.payload = null;
    }
  }

  public boolean result() {
    return result;
  }

  @SuppressFBWarnings("EI_EXPOSE_REP")
  public byte[] getPayload() {
    return payload;
  }
}
