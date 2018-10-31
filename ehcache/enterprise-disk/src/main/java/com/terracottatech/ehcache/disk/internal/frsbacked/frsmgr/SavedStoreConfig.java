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
package com.terracottatech.ehcache.disk.internal.frsbacked.frsmgr;

import org.ehcache.core.spi.store.Store;

import com.terracottatech.ehcache.common.frs.metadata.ChildConfiguration;
import com.terracottatech.ehcache.common.frs.metadata.FrsDataLogIdentifier;
import com.terracottatech.ehcache.common.frs.LocalEncoder;
import com.terracottatech.ehcache.common.frs.metadata.MetadataConfiguration;

import java.nio.ByteBuffer;

import static com.terracottatech.ehcache.common.frs.FrsCodecUtils.CHAR_SIZE;
import static com.terracottatech.ehcache.common.frs.FrsCodecUtils.INT_SIZE;
import static com.terracottatech.ehcache.common.frs.FrsCodecUtils.LONG_SIZE;
import static com.terracottatech.ehcache.common.frs.FrsCodecUtils.extractString;
import static com.terracottatech.ehcache.common.frs.FrsCodecUtils.putString;

/**
 * Container for saved store configuration against each store. Used for validation purposes
 * when a store is re-attached post recovery of FRS.
 *
 * @author RKAV
 */
@SuppressWarnings("WeakerAccess")
public final class SavedStoreConfig implements LocalEncoder, ChildConfiguration<String> {
  private static final String DIVIDER = ":";

  private int storeIndex;
  private final long sizeInBytes;
  private final String keyTypeName;
  private final String valueTypeName;
  private final FrsDataLogIdentifier dataLogId;
  private final String parentId;

  private transient ClassLoader storedClassLoader;

  public SavedStoreConfig(FrsDataLogIdentifier dataLogId, String parentId,
                          Store.Configuration<?, ?> storeConfig, long sizeInBytes) {
    this.keyTypeName = storeConfig.getKeyType().getName();
    this.valueTypeName = storeConfig.getValueType().getName();
    this.sizeInBytes = sizeInBytes;
    this.dataLogId = dataLogId;
    this.storeIndex = 0;
    this.parentId = parentId;
    this.storedClassLoader = storeConfig.getClassLoader();
  }

  @SuppressWarnings("unused")
  public SavedStoreConfig(ByteBuffer bb) {
    this.storeIndex = bb.getInt();
    this.sizeInBytes = bb.getLong();
    this.dataLogId = FrsDataLogIdentifier.extractDataLogIdentifier(bb);
    this.keyTypeName = extractString(bb);
    this.valueTypeName = extractString(bb);
    this.parentId = extractString(bb);
  }

  @SuppressWarnings({ "cast", "RedundantCast" })
  @Override
  public ByteBuffer encode() {
    int size = LONG_SIZE + INT_SIZE * 4;
    size += dataLogId.totalSize();
    size += keyTypeName.length() * CHAR_SIZE;
    size += valueTypeName.length() * CHAR_SIZE;
    size += parentId.length() * CHAR_SIZE;
    ByteBuffer buffer = ByteBuffer.allocate(size);
    buffer.putInt(storeIndex);
    buffer.putLong(getSizeInBytes());
    FrsDataLogIdentifier.putDataLogIdentifier(buffer, dataLogId);
    putString(buffer, keyTypeName);
    putString(buffer, valueTypeName);
    putString(buffer, parentId);
    return (ByteBuffer)buffer.flip();       // made redundant in Java 9/10
  }

  @Override
  public String toString() {
    return dataLogId + DIVIDER + keyTypeName + DIVIDER + valueTypeName + DIVIDER +
           sizeInBytes + DIVIDER + parentId;
  }

  long getSizeInBytes() {
    return sizeInBytes;
  }

  String getFrsId() {
    return dataLogId.getDataLogName();
  }

  @Override
  public String getParentId() {
    return parentId;
  }

  @Override
  public FrsDataLogIdentifier getDataLogIdentifier() {
    return dataLogId;
  }

  @Override
  public int getObjectIndex() {
    return storeIndex;
  }

  @Override
  public void setObjectIndex(int index) {
    this.storeIndex = index;
  }

  @Override
  public void validate(MetadataConfiguration newMetadataConfiguration) {
    if (!(newMetadataConfiguration instanceof SavedStoreConfig)) {
      throw new IllegalArgumentException("Mismatched types existing for given cache ");
    }
    SavedStoreConfig otherConfig = (SavedStoreConfig)newMetadataConfiguration;
    ClassLoader classLoader = otherConfig.storedClassLoader;
    try {
      if (!(getKeyType(classLoader).equals(otherConfig.getKeyType(classLoader)))) {
        throw new IllegalArgumentException(
            "Key type in FRS '" + getKeyType(classLoader) +
            "' is not the same as the key type in current configuration '" + otherConfig.getKeyType(classLoader));
      }
    } catch (ClassNotFoundException cnfe) {
      throw new IllegalStateException("Class not found for key type");
    }
    try {
      if (!(getValueType(classLoader).equals(otherConfig.getValueType(classLoader)))) {
        throw new IllegalArgumentException(
            "Value type in FRS '" + getKeyType(classLoader) +
            "' is not the same as the value type in current configuration '" + otherConfig.getKeyType(classLoader));
      }
    } catch (ClassNotFoundException cnfe) {
      throw new IllegalStateException("Class not found for value type");
    }
    if (getSizeInBytes() != otherConfig.getSizeInBytes()) {
      throw new IllegalArgumentException("Cannot change FRS resource pool size once FRS is configured for this store");
    }
    if (!getFrsId().equals(otherConfig.getFrsId())) {
      throw new IllegalArgumentException("Cannot change FRS Container once FRS is configured for this store");
    }
  }

  private Class<?> getKeyType(final ClassLoader classLoader) throws ClassNotFoundException {
    return Class.forName(keyTypeName, false, classLoader);
  }

  private Class<?> getValueType(final ClassLoader classLoader) throws ClassNotFoundException {
    return Class.forName(valueTypeName, false, classLoader);
  }
}
