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
package com.terracottatech.ehcache.common.frs.metadata;

import com.tc.classloader.CommonComponent;
import org.terracotta.runnel.Struct;
import org.terracotta.runnel.StructBuilder;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.StructEncoder;

import java.io.Serializable;
import java.nio.ByteBuffer;

import static com.terracottatech.ehcache.common.frs.FrsCodecUtils.CHAR_SIZE;
import static com.terracottatech.ehcache.common.frs.FrsCodecUtils.INT_SIZE;
import static com.terracottatech.ehcache.common.frs.FrsCodecUtils.extractString;
import static com.terracottatech.ehcache.common.frs.FrsCodecUtils.putString;

/**
 * Uniquely identifies a data log.
 */
@CommonComponent
public class FrsDataLogIdentifier extends FrsContainerIdentifier implements Serializable {

  private static final long serialVersionUID = -6004610903760662347L;

  private static final String ROOT_PATH_NAME = "rootPathName";
  private static final String CONTAINER_NAME = "containerName";
  private static final String DATA_LOG_NAME = "dataLogName";

  public static final Struct STRUCT = StructBuilder.newStructBuilder()
    .string(ROOT_PATH_NAME, 10)
    .string(CONTAINER_NAME, 20)
    .string(DATA_LOG_NAME, 30).build();

  private final String dataLogName;

  public FrsDataLogIdentifier(String rootPathName, String containerName, String dataLogName) {
    super(rootPathName, containerName);
    this.dataLogName = dataLogName;
  }

  /**
   * Returns name of the data log name.
   *
   * @return name of the data log
   */
  public String getDataLogName() {
    return dataLogName;
  }

  public int totalSize() {
    return super.totalSize() + (getDataLogName().length() * CHAR_SIZE) + INT_SIZE;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    FrsDataLogIdentifier that = (FrsDataLogIdentifier)o;

    return dataLogName.equals(that.dataLogName);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + dataLogName.hashCode();
    return result;
  }

  public static FrsDataLogIdentifier extractDataLogIdentifier(ByteBuffer bb) {
    String rootName = extractString(bb);
    String containerName = extractString(bb);
    String dataLogName = extractString(bb);
    return new FrsDataLogIdentifier(rootName, containerName, dataLogName);
  }

  public static void putDataLogIdentifier(ByteBuffer bb, FrsDataLogIdentifier dataLogId) {
    putString(bb, dataLogId.getRootPathName());
    putString(bb, dataLogId.getContainerName());
    putString(bb, dataLogId.getDataLogName());
  }

  ByteBuffer encode() {
    final StructEncoder<Void> encoder = STRUCT.encoder();
    encode(encoder);
    ByteBuffer encodedBytes = encoder.encode();
    encodedBytes.flip();
    return encodedBytes;
  }

  public void encode(StructEncoder<?> structEncoder) {
    structEncoder.string(ROOT_PATH_NAME, getRootPathName())
      .string(CONTAINER_NAME, getContainerName())
      .string(DATA_LOG_NAME, getDataLogName());
  }

  static FrsDataLogIdentifier decode(ByteBuffer byteBuffer) {
    return decode(STRUCT.decoder(byteBuffer));
  }

  public static FrsDataLogIdentifier decode(StructDecoder<?> structDecoder) {
    final String rootPathName = structDecoder.string(ROOT_PATH_NAME);
    final String containerName = structDecoder.string(CONTAINER_NAME);
    final String dataLogName = structDecoder.string(DATA_LOG_NAME);

    return new FrsDataLogIdentifier(rootPathName, containerName, dataLogName);
  }
}
