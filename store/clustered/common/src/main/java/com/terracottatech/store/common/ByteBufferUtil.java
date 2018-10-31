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
package com.terracottatech.store.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.List;

public class ByteBufferUtil {

  public static <T> ByteBuffer serialize(T data) {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

    try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {
      objectOutputStream.writeObject(data);
    } catch (IOException e) {
      throw new RuntimeException("Failed to serialize : " + data, e);
    }

    ByteBuffer byteBuffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
    return byteBuffer;
  }

  public static <T> T deserializeWhiteListed(ByteBuffer buf, Class<T> target, List<Class<?>> whiteListedClasses) {
    byte[] bytes = new byte[buf.remaining()];
    buf.get(bytes);
    ByteArrayInputStream bis = new ByteArrayInputStream(bytes);

    try (WhiteListObjectInputStream objectInputStream = new WhiteListObjectInputStream(bis, whiteListedClasses)) {
      return target.cast(objectInputStream.readObject());
    } catch (InvalidClassException e) {
      throw new RuntimeException(e.getMessage());
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException("Failed to deserialize : " + buf + " to an instance of type " + target, e);
    }
  }
}
