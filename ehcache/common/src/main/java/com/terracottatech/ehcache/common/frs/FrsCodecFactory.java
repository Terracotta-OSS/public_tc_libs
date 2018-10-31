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
package com.terracottatech.ehcache.common.frs;

import org.terracotta.offheapstore.util.ByteBufferInputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.terracottatech.ehcache.common.frs.FrsCodecUtils.BYTE_SIZE;
import static com.terracottatech.ehcache.common.frs.FrsCodecUtils.CHAR_SIZE;
import static com.terracottatech.ehcache.common.frs.FrsCodecUtils.DOUBLE_SIZE;
import static com.terracottatech.ehcache.common.frs.FrsCodecUtils.FLOAT_SIZE;
import static com.terracottatech.ehcache.common.frs.FrsCodecUtils.INT_SIZE;
import static com.terracottatech.ehcache.common.frs.FrsCodecUtils.LONG_SIZE;

/**
 * Codec factory that returns a codec, given a type.
 *
 * Note: Map a singleton codec per type as codecs can be static and so we can avoid the allocation and GC overhead.
 *
 * @author RKAV
 */
public class FrsCodecFactory {
  private static final FrsCodec<?> DEFAULT_CODEC = new DefaultCodec();
  private static final Map<Class<?>, FrsCodec<?>> codecMap;
  static {
    Map<Class<?>, FrsCodec<?>> tmpMap = new HashMap<>();
    tmpMap.put(Integer.class, new IntegerCodec());
    tmpMap.put(String.class, new StringCodec());
    tmpMap.put(Float.class, new FloatCodec());
    tmpMap.put(Double.class, new DoubleCodec());
    tmpMap.put(Long.class, new LongCodec());
    tmpMap.put(Byte.class, new ByteCodec());
    tmpMap.put(Character.class, new CharCodec());
    codecMap = Collections.unmodifiableMap(tmpMap);
  }
  private static final ConcurrentMap<Class<?>, GenericCodec<? extends LocalEncoder>> genericCodecMap =
      new ConcurrentHashMap<>();

  // keep a different copy to avoid map lookup for some basic codecs used in heavily used generic methods
  private static final FrsCodec<String> STRING_FRS_CODEC = new StringCodec();
  private static final FrsCodec<Long> LONG_FRS_CODEC = new LongCodec();

  public FrsCodecFactory() {
  }

  public static ByteBuffer toByteBuffer(final String value) {
    return STRING_FRS_CODEC.encode(value);
  }

  static Long bufferToLong(final ByteBuffer bb) {
    return LONG_FRS_CODEC.decode(bb);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  static <T> FrsCodec<T> getCodec(Class<T> clazz) {
    FrsCodec<?> codecToUse = codecMap.get(clazz);
    if (codecToUse != null) {
      return (FrsCodec<T>)codecToUse;
    }
    if (LocalEncoder.class.isAssignableFrom(clazz)) {
      GenericCodec<? extends LocalEncoder> foundOrNewCodec = genericCodecMap.get(clazz);
      GenericCodec<? extends LocalEncoder> existingCodec = null;
      if (foundOrNewCodec == null) {
        foundOrNewCodec = new GenericCodec(clazz);
        existingCodec = genericCodecMap.putIfAbsent(clazz, foundOrNewCodec);
      }
      return (FrsCodec<T>)(existingCodec == null ? foundOrNewCodec : existingCodec);
    }
    return (FrsCodec<T>)DEFAULT_CODEC;
  }

  private static <T> String assertMessage(Class<T> type, Exception e) {
    return "Unexpected Internal Error: " + type.getName() +
           " class instantiation failed with " + e.getMessage();
  }

  private static final class DefaultCodec implements FrsCodec<Object> {
    private DefaultCodec() {
    }

    @SuppressWarnings("ThrowFromFinallyBlock")
    public ByteBuffer encode(Object item) {
      if (item == null) {
        return null;
      }
      try (ByteArrayOutputStream bout = new ByteArrayOutputStream(); ObjectOutputStream oout = new ObjectOutputStream(bout)) {
        oout.writeObject(item);
        return ByteBuffer.wrap(bout.toByteArray());
      } catch (IOException e) {
        throw new AssertionError(e);
      }
    }

    @SuppressWarnings("ThrowFromFinallyBlock")
    public Object decode(ByteBuffer data) {
      try (ByteBufferInputStream bin = new ByteBufferInputStream(data.duplicate()); ObjectInputStream oin = new ObjectInputStream(bin)) {
        Object o = oin.readObject();
        return o;
      } catch (IOException | ClassNotFoundException e) {
        throw new AssertionError(e);
      }
    }
  }

  private static final class StringCodec implements FrsCodec<String> {
    private StringCodec() {
    }

    public ByteBuffer encode(String value) {
      ByteBuffer buffer = ByteBuffer.allocate(value.length() * CHAR_SIZE);
      buffer.asCharBuffer().put(value);
      return buffer;
    }

    public String decode(ByteBuffer data) {
      return data.asCharBuffer().toString();
    }
  }

  /**
   * Abstract bases class for all basic type codecs.
   *
   * @param <T> any of the basic types
   */
  private static abstract class BasicTypeCodec<T> implements FrsCodec<T> {
    private BasicTypeCodec() {
    }

    @SuppressWarnings({ "cast", "RedundantCast" })
    @Override
    public ByteBuffer encode(T value) {
      ByteBuffer buffer = ByteBuffer.allocate(getSize());
      putValue(buffer, value);
      return (ByteBuffer)buffer.flip();       // made redundant in Java 9/10
    }

    @Override
    public T decode(ByteBuffer data) {
      int p = data.position();
      try {
        return getData(data);
      } finally {
        data.position(p);
      }
    }

    abstract T getData(ByteBuffer data);
    abstract void putValue(ByteBuffer buffer, T value);
    abstract int getSize();
  }

  private static final class IntegerCodec extends BasicTypeCodec<Integer> {
    private IntegerCodec() {
    }

    @Override
    Integer getData(ByteBuffer data) {
      return data.getInt();
    }

    @Override
    void putValue(ByteBuffer buffer, Integer value) {
      buffer.putInt(value);
    }

    @Override
    int getSize() {
      return INT_SIZE;
    }
  }

  private static final class FloatCodec extends BasicTypeCodec<Float> {
    private FloatCodec() {
    }

    @Override
    Float getData(ByteBuffer data) {
      return data.getFloat();
    }

    @Override
    void putValue(ByteBuffer buffer, Float value) {
      buffer.putFloat(value);
    }

    @Override
    int getSize() {
      return FLOAT_SIZE;
    }
  }

  private static final class DoubleCodec extends BasicTypeCodec<Double> {
    private DoubleCodec() {
    }

    @Override
    Double getData(ByteBuffer data) {
      return data.getDouble();
    }

    @Override
    void putValue(ByteBuffer buffer, Double value) {
      buffer.putDouble(value);
    }

    @Override
    int getSize() {
      return DOUBLE_SIZE;
    }
  }

  private static final class LongCodec extends BasicTypeCodec<Long> {
    private LongCodec() {
    }

    @Override
    Long getData(ByteBuffer data) {
      return data.getLong();
    }

    @Override
    void putValue(ByteBuffer buffer, Long value) {
      buffer.putLong(value);
    }

    @Override
    int getSize() {
      return LONG_SIZE;
    }
  }

  private static final class ByteCodec extends BasicTypeCodec<Byte> {
    private ByteCodec() {
    }

    @Override
    Byte getData(ByteBuffer data) {
      return data.get();
    }

    @Override
    void putValue(ByteBuffer buffer, Byte value) {
      buffer.put(value);
    }

    @Override
    int getSize() {
      return BYTE_SIZE;
    }
  }

  private static final class CharCodec extends BasicTypeCodec<Character> {
    private CharCodec() {
    }

    @Override
    Character getData(ByteBuffer data) {
      return data.getChar();
    }

    @Override
    void putValue(ByteBuffer buffer, Character value) {
      buffer.putChar(value);
    }

    @Override
    int getSize() {
      return CHAR_SIZE;
    }
  }

  private static final class GenericCodec<T extends LocalEncoder> implements FrsCodec<T> {
    private final Class<T> type;
    private GenericCodec(Class<T> type) {
      this.type = type;
    }

    public ByteBuffer encode(T obj) {
      return obj.encode();
    }

    public T decode(ByteBuffer buf) {
      try {
        return type.getDeclaredConstructor(ByteBuffer.class).newInstance(buf.duplicate());
      } catch (InstantiationException | NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
        // these are internally controlled types..it is an internal error if any exception is thrown
        throw new AssertionError(assertMessage(type, e));
      }
    }
  }
}
