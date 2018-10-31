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
package com.terracottatech.sovereign.impl.persistence.base;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * This is a placeholder. I don't really like what it would imply everywhere else,
 * which would mean the creation of one of these for every BufferContainer.get()
 * call, which seems like a lot of noise.
 *
 * @author cschanck
 **/
public class CloseableByteBuffer implements AutoCloseable {

  public static final Closeable NULL_CLOSEABLE = () -> {
  };
  private final ByteBuffer buf;
  private final Closeable closeable;

  public CloseableByteBuffer(Object closeable, ByteBuffer buf) {
    this.buf = buf;
    if (closeable != null && closeable instanceof Closeable) {
      this.closeable = (Closeable) closeable;
    } else {
      this.closeable = NULL_CLOSEABLE;
    }
  }

  public ByteBuffer getBuffer() {
    return this.buf;
  }

  @Override
  public void close() {
    try {
      closeable.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
