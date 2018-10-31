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

import com.terracottatech.sovereign.impl.SovereignDatasetDescriptionImpl;
import com.terracottatech.sovereign.impl.dataset.metadata.PersistableSchemaList;
import com.terracottatech.sovereign.impl.utils.CachingSequence;
import com.terracottatech.sovereign.time.PersistableTimeReferenceGenerator;
import com.terracottatech.sovereign.time.TimeReference;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;

/**
 * @author cschanck
 **/
public class MetadataKey<T extends Serializable> implements Serializable {
//TODO: Make sure all this gets persisted
  private static final long serialVersionUID = 1705709462186902619L;

  /**
   * Provides an enum-like API since enums currently don't support
   * generic types.
   */
  public static class Tag<T extends Serializable> implements Serializable {

    private static final long serialVersionUID = 6439705009613637907L;

    @SuppressWarnings("unchecked")
    public static final Tag<SovereignDatasetDescriptionImpl<?, ?>> DATASET_DESCR = (Tag)
            new Tag<>(SovereignDatasetDescriptionImpl.class, 0);

    public static final Tag<CachingSequence> CACHING_SEQUENCE = new Tag<>(
            CachingSequence.class, 1);

    @SuppressWarnings("unchecked")
    static final Tag<PersistableTimeReferenceGenerator<?>> PERSISTABLE_TIMEGEN = (Tag)
            new Tag<>(PersistableTimeReferenceGenerator.class, 2);

    public static final Tag<PersistableSchemaList> SCHEMA = new Tag<>(
            PersistableSchemaList.class, 3);

    static final Tag<Boolean> PENDING_DELETE = new Tag<>(
            Boolean.class, 4);

    private final Class<T> klass;
    private final int ordinal;

    private Tag(Class<T> klass, int ordinal) {
      this.klass = klass;
      this.ordinal = ordinal;
    }

    public MetadataKey<T> keyFor(UUID uuid) {
      return new MetadataKey<>(uuid, this);
    }

    public T cast(Object o) {
      return klass.cast(o);
    }

    public void validate(Object o) {
      if (!klass.isInstance(o)) {
        throw new IllegalArgumentException();
      }
    }

    public static Tag<?>[] values() {
      return new Tag<?>[]{
              DATASET_DESCR,
              CACHING_SEQUENCE,
              PERSISTABLE_TIMEGEN,
              SCHEMA,
              PENDING_DELETE
      };
    }

    public int ordinal() {
      return ordinal;
    }
  }

  private final UUID uuid;
  private final Tag<?> tag;
  private transient ByteBuffer buffer = null;

  private MetadataKey(UUID uuid, Tag<?> tag) {
    this.uuid = uuid;
    this.tag = tag;
  }

  public MetadataKey(ByteBuffer source) {
    ByteBuffer slice = source.slice();
    long msb = slice.getLong();
    long lsb = slice.getLong();
    int t = slice.getInt();
    tag = Tag.values()[t];
    uuid = new UUID(msb, lsb);
  }

  public UUID getUUID() {
    return uuid;
  }

  public Tag<?> getTag() {
    return tag;
  }

  private ByteBuffer makeBuffer(UUID dsUUID, Tag<?> tag) {
    ByteBuffer buf = ByteBuffer.allocate(byteFootprint());
    buf.putLong(dsUUID.getMostSignificantBits());
    buf.putLong(dsUUID.getLeastSignificantBits());
    buf.putInt(tag.ordinal());
    buf.clear();
    return buf;
  }

  ByteBuffer toBuffer() {
    if (buffer == null) {
      ByteBuffer buf = makeBuffer(uuid, tag);
      buffer = buf.asReadOnlyBuffer();
    }
    return buffer.slice();
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    in.defaultReadObject();
    buffer = makeBuffer(uuid, tag);
  }

  static int byteFootprint() {
    return Long.BYTES * 2 + Integer.BYTES;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    MetadataKey<?> key = (MetadataKey<?>) o;

    if (!uuid.equals(key.uuid)) {
      return false;
    }
    return tag == key.tag;

  }

  @Override
  public int hashCode() {
    int result = uuid.hashCode();
    result = 31 * result + tag.hashCode();
    return result;
  }

  void putInMap(Map<MetadataKey<?>, Object> map, Object value) {
    this.tag.validate(value);
    map.put(this, value);
  }

  void deleteInMap(Map<MetadataKey<?>, Object> map) {
    map.remove(this);
  }

  boolean keyInMap(Map<MetadataKey<?>, Object> map) {
    return map.containsKey(this);
  }

  @SuppressWarnings("unchecked")
  T getInMap(Map<MetadataKey<?>, Object> map) {
    return (T) map.get(this);
  }

  public static class Facade<Z extends TimeReference<Z>> {
    private final UUID uuid;
    private final Map<MetadataKey<?>, Object> metamap;
    private final MetadataKey<PersistableTimeReferenceGenerator<?>> timegenKey;
    private final MetadataKey<CachingSequence> sequenceKey;
    private final MetadataKey<SovereignDatasetDescriptionImpl<?, ?>> descrKey;
    private final MetadataKey<PersistableSchemaList> schemaKey;
    private final MetadataKey<Boolean> pendingDeleteKey;

    Facade(UUID uuid, Map<MetadataKey<?>, Object> map) {
      this.uuid = uuid;
      this.metamap = map;
      this.sequenceKey = Tag.CACHING_SEQUENCE.keyFor(uuid);
      this.descrKey = Tag.DATASET_DESCR.keyFor(uuid);
      this.timegenKey = Tag.PERSISTABLE_TIMEGEN.keyFor(uuid);
      this.schemaKey = Tag.SCHEMA.keyFor(uuid);
      this.pendingDeleteKey = Tag.PENDING_DELETE.keyFor(uuid);
    }

    public UUID getUUID() {
      return uuid;
    }

    public CachingSequence sequence() {
      return sequenceKey.getInMap(metamap);
    }

    public void setSequence(CachingSequence seq) {
      sequenceKey.putInMap(metamap, seq);
    }

    @SuppressWarnings("unchecked")
    public SovereignDatasetDescriptionImpl<?, Z> description() {
      return (SovereignDatasetDescriptionImpl<?, Z>) descrKey.getInMap(metamap);
    }

    public void setDescription(SovereignDatasetDescriptionImpl<?, ?> descr) {
      descrKey.putInMap(metamap, descr);
    }

    @SuppressWarnings("unchecked")
    PersistableTimeReferenceGenerator<Z> timeGenerator() {
      return (PersistableTimeReferenceGenerator<Z>) timegenKey.getInMap(metamap);
    }

    void setTimegen(PersistableTimeReferenceGenerator<Z> tgen) {
      timegenKey.putInMap(metamap, tgen);
    }

    PersistableSchemaList schemaBackend() {
      return schemaKey.getInMap(metamap);
    }

    void setSchemaBackend(PersistableSchemaList backend) {
      schemaKey.putInMap(metamap, backend);
    }

    public void setPendingDelete() {
      schemaKey.putInMap(metamap, pendingDeleteKey);
    }

    boolean isPendingDelete() {
      Boolean val = pendingDeleteKey.getInMap(metamap);
      if (val != null) {
        return val;
      }
      return false;
    }
  }
}
