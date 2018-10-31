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
package com.terracottatech.sovereign.impl.memory.storageengines;

import com.terracottatech.sovereign.common.utils.StringTool;

import java.io.Serializable;
import java.io.UTFDataFormatException;
import java.nio.ByteBuffer;
import java.util.Comparator;

/**
 * @author mscott
 */
public class PrimitivePortabilityImpl<T> implements Comparator<T>, Serializable, PrimitivePortability<T> {
  private static final long serialVersionUID = 1L;
  private final Primitive converter;

  public static final PrimitivePortability<Boolean> BOOLEAN = new PrimitivePortabilityImpl<>(Boolean.class);
  public static final PrimitivePortability<Byte> BYTE = new PrimitivePortabilityImpl<>(Byte.class);
  public static final PrimitivePortability<Character> CHAR = new PrimitivePortabilityImpl<>(Character.class);
  public static final PrimitivePortability<Integer> INT = new PrimitivePortabilityImpl<>(Integer.class);
  public static final PrimitivePortability<Long> LONG = new PrimitivePortabilityImpl<>(Long.class);
  public static final PrimitivePortability<Double> DOUBLE = new PrimitivePortabilityImpl<>(Double.class);
  public static final PrimitivePortability<String> STRING = new PrimitivePortabilityImpl<>(String.class);
  public static final PrimitivePortability<byte[]> BYTES = new PrimitivePortabilityImpl<>(byte[].class);

  @SuppressWarnings("unchecked")
  public static <T> PrimitivePortabilityImpl<T> factory(Class<T> type) {
    if (type == Boolean.class) {
      return (PrimitivePortabilityImpl<T>) BOOLEAN;
    } else if (type == Byte.class) {
      return (PrimitivePortabilityImpl<T>) BYTE;
    } else if (type == Character.class) {
      return (PrimitivePortabilityImpl<T>) CHAR;
    } else if (type == Integer.class) {
      return (PrimitivePortabilityImpl<T>) INT;
    } else if (type == Long.class) {
      return (PrimitivePortabilityImpl<T>) LONG;
    } else if (type == Double.class) {
      return (PrimitivePortabilityImpl<T>) DOUBLE;
    } else if (type == String.class) {
      return (PrimitivePortabilityImpl<T>) STRING;
    } else if (type == byte[].class) {
      return (PrimitivePortabilityImpl<T>) BYTES;
    }
    throw new IllegalArgumentException();
  }

  private PrimitivePortabilityImpl(Class<T> type) {
    this.converter = Primitive.forType(type);
  }

  @Override
  public boolean isInstance(Object o) {
    return converter.isInstance(o);
  }

  @Override
  public ByteBuffer encode(T object) {
    return converter.encode(object);
  }

  @Override
  public void encode(Object obj, ByteBuffer dest) {
    converter.encode(obj, dest);
  }

  @Override
  @SuppressWarnings("unchecked")
  public T decode(ByteBuffer buf) {
    return (T) converter.decode(buf);
  }

  @Override
  public int compare(T o1, T o2) {
    return converter.compare(o1, o2);
  }

  @Override
  public int spaceNeededToEncode(Object obj) {
    return converter.spaceNeededToEncode(obj);
  }

  @Override
  public boolean isFixedSize() {
    return converter.isFixedSize();
  }

  @SuppressWarnings("unchecked")
  @Override
  public Class<T> getType() {
    return (Class<T>) converter.getType();
  }

  enum Primitive {
    BOOLEAN(Boolean.class, 1) {
      public void encode(Object b, ByteBuffer dest) {
        if ((boolean) b) {
          dest.put((byte) 1);
        } else {
          dest.put((byte) 0);
        }
      }

      @Override
      public ByteBuffer encode(Object bt) {
        ByteBuffer coded = ByteBuffer.allocate(Byte.BYTES);
        if ((boolean) bt) {
          coded.put((byte) 1).flip();
        } else {
          coded.put((byte) 0).flip();
        }
        return coded;
      }

      @Override
      public Boolean decode(ByteBuffer bt) {
        return bt.get() > 0 ? Boolean.TRUE : Boolean.FALSE;
      }

      @Override
      public boolean isInstance(Object o) {
        return Boolean.class.isInstance(o);
      }

      @Override
      public int compare(Object o1, Object o2) {
        return Boolean.compare((Boolean) o1, (Boolean) o2);
      }

    },
    BYTE(Byte.class, 1) {
      @Override
      public ByteBuffer encode(Object bt) {
        ByteBuffer coded = ByteBuffer.allocate(Byte.BYTES);
        coded.put((byte) bt).flip();
        return coded;
      }

      @Override
      public void encode(Object obj, ByteBuffer dest) {
        dest.put((byte) obj);
      }

      @Override
      public Byte decode(ByteBuffer bt) {
        return bt.get();
      }

      @Override
      public boolean isInstance(Object o) {
        return Byte.class.isInstance(o);
      }

      @Override
      public int compare(Object o1, Object o2) {
        return Byte.compare((Byte) o1, (Byte) o2);
      }
    },
    CHAR(Character.class, Character.BYTES) {
      @Override
      public ByteBuffer encode(Object bt) {
        ByteBuffer coded = ByteBuffer.allocate(Character.BYTES);
        coded.putChar((char) bt).flip();
        return coded;
      }

      @Override
      public void encode(Object obj, ByteBuffer dest) {
        dest.putChar((char) obj);
      }

      @Override
      public Character decode(ByteBuffer bt) {
        return bt.getChar();
      }

      @Override
      public boolean isInstance(Object o) {
        return Character.class.isInstance(o);
      }

      @Override
      public int compare(Object o1, Object o2) {
        return Character.compare((Character) o1, (Character) o2);
      }

    },
    INT(Integer.class, Integer.BYTES) {
      @Override
      public ByteBuffer encode(Object bt) {
        ByteBuffer coded = ByteBuffer.allocate(Integer.BYTES);
        coded.putInt((int) bt).flip();
        return coded;
      }

      @Override
      public void encode(Object obj, ByteBuffer dest) {
        dest.putInt((int) obj);
      }

      @Override
      public Integer decode(ByteBuffer bt) {
        return bt.getInt();
      }

      @Override
      public boolean isInstance(Object o) {
        return Integer.class.isInstance(o);
      }

      @Override
      public int compare(Object o1, Object o2) {
        return Integer.compare((Integer) o1, (Integer) o2);
      }
    },
    LONG(Long.class, Long.BYTES) {
      @Override
      public ByteBuffer encode(Object bt) {
        ByteBuffer coded = ByteBuffer.allocate(Long.BYTES);
        coded.putLong((long) bt).flip();
        return coded;
      }

      @Override
      public void encode(Object obj, ByteBuffer dest) {
        dest.putLong((long) obj);
      }

      @Override
      public Long decode(ByteBuffer bt) {
        return bt.getLong();
      }

      @Override
      public boolean isInstance(Object o) {
        return Long.class.isInstance(o);
      }

      @Override
      public int compare(Object o1, Object o2) {
        return Long.compare((Long) o1, (Long) o2);
      }
    },
    DOUBLE(Double.class, Double.BYTES) {
      @Override
      public ByteBuffer encode(Object bt) {
        ByteBuffer coded = ByteBuffer.allocate(Double.BYTES);
        coded.putDouble((double) bt).flip();
        return coded;
      }

      @Override
      public void encode(Object obj, ByteBuffer dest) {
        dest.putDouble((double) obj);
      }

      @Override
      public Double decode(ByteBuffer bt) {
        return bt.getDouble();
      }

      @Override
      public boolean isInstance(Object o) {
        return Double.class.isInstance(o);
      }

      @Override
      public int compare(Object o1, Object o2) {
        return Double.compare((Double) o1, (Double) o2);
      }
    },
    STRING(String.class) {
      @Override
      public int spaceNeededToEncode(Object obj) {
        return StringTool.worstCaseByteArraySize(obj.toString());
      }

      @Override
      public ByteBuffer encode(Object base) {
        String object = base.toString();
        ByteBuffer coded = ByteBuffer.allocate(StringTool.worstCaseByteArraySize(object));
        StringTool.putUTF(coded, object);
        coded.flip();
        return coded;
      }

      @Override
      public void encode(Object obj, ByteBuffer dest) {
        StringTool.putUTF(dest, obj.toString());
      }

      @Override
      public String decode(ByteBuffer buf) {
        try {
          return StringTool.getUTF(buf);
        } catch (UTFDataFormatException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public boolean isInstance(Object o) {
        return String.class.isInstance(o);
      }

      @Override
      public int compare(Object o1, Object o2) {
        return o1.toString().compareTo(o2.toString());
      }
    },
    BYTES(byte[].class) {
      @Override
      public ByteBuffer encode(Object base) {
        byte[] object = (byte[]) base;
        ByteBuffer coded = ByteBuffer.allocate(object.length);
        coded.put(object).clear();
        return coded;
      }

      @Override
      public int spaceNeededToEncode(Object obj) {
        return ((byte[]) obj).length;
      }

      @Override
      public void encode(Object obj, ByteBuffer dest) {
        dest.put((byte[]) obj);
      }

      @Override
      public byte[] decode(ByteBuffer buf) {
        byte[] val = new byte[buf.remaining()];
        buf.get(val);
        return val;
      }

      @Override
      public boolean isInstance(Object o) {
        return byte[].class.isInstance(o);
      }

      @Override
      public int compare(Object o1, Object o2) {
        byte[] left = (byte[]) o1;
        byte[] right = (byte[]) o2;
        for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++) {
          int a = (left[i] & 0xff);
          int b = (right[j] & 0xff);
          if (a != b) {
            return a - b;
          }
        }
        return left.length - right.length;
      }

    };

    private final boolean isFixedSize;
    private final int fixedSize;
    private final Class<?> clz;

    Primitive(Class<?> clz) {
      this.clz = clz;
      isFixedSize = false;
      fixedSize = -1;
    }

    Primitive(Class<?> clz, int fixedSize) {
      this.clz = clz;
      this.isFixedSize = true;
      this.fixedSize = fixedSize;
    }

    public static Primitive forType(Class<?> type) {
      if (type.isAssignableFrom(String.class)) {
        return STRING;
      } else if (type.isAssignableFrom(Long.class)) {
        return LONG;
      } else if (type.isAssignableFrom(Integer.class)) {
        return INT;
      } else if (type.isAssignableFrom(Character.class)) {
        return CHAR;
      } else if (type.isAssignableFrom(Double.class)) {
        return DOUBLE;
      } else if (type.isAssignableFrom(byte[].class)) {
        return BYTES;
      } else if (type.isAssignableFrom(Byte.class)) {
        return BYTE;
      } else if (type.isAssignableFrom(Boolean.class)) {
        return BOOLEAN;
      }
      throw new IllegalArgumentException();
    }

    public abstract Object decode(ByteBuffer buf);

    public abstract ByteBuffer encode(Object obj);

    public abstract void encode(Object obj, ByteBuffer dest);

    public abstract boolean isInstance(Object o);

    public abstract int compare(Object o1, Object o2);

    public Class<?> getType() {
      return clz;
    }

    public int spaceNeededToEncode(Object obj) {
      if (isFixedSize()) {
        return fixedSize;
      }
      throw new UnsupportedOperationException();
    }

    public boolean isFixedSize() {
      return isFixedSize;
    }

  }

  @Override
  public boolean equals(Object object, ByteBuffer buffer) {
    if (converter.isInstance(object)) {
      return object.equals(converter.decode(buffer));
    }
    return false;
  }
}
