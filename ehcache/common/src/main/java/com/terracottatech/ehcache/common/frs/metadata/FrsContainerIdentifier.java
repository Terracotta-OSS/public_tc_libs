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
import org.terracotta.runnel.decoding.PrimitiveDecodingSupport;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.PrimitiveEncodingSupport;
import org.terracotta.runnel.encoding.StructEncoder;

import java.io.Serializable;
import java.nio.ByteBuffer;

import static com.terracottatech.ehcache.common.frs.FrsCodecUtils.CHAR_SIZE;
import static com.terracottatech.ehcache.common.frs.FrsCodecUtils.INT_SIZE;

/**
 * Uniquely identifies a FRS log container. An <i>FRS log container</i> contains a metadata log and one or more data
 * logs.
 * <p>
 * This ensures that configuration metadata ONLY stores logical names and NOT hardcoded paths, allowing better
 * portability across servers and root path changes.
 */
@CommonComponent
public class FrsContainerIdentifier implements Serializable {

  private static final long serialVersionUID = 4671937351165531738L;

  private static final String ROOT_PATH_NAME = "rootPathName";
  private static final String CONTAINER_NAME = "containerName";

  public static final Struct STRUCT = StructBuilder.newStructBuilder()
    .string(ROOT_PATH_NAME, 10)
    .string(CONTAINER_NAME, 20).build();

  private final String rootPathName;
  private final String containerName;

  public FrsContainerIdentifier(String rootPathName, String containerName) {
    this.rootPathName = rootPathName;
    this.containerName = containerName;
  }

  /**
   * Returns the logical name (a.k.a alias) of the root path of container identified by {@code this}.
   *
   * @return a logical name
   */
  public String getRootPathName() {
    return rootPathName;
  }

  /**
   * Returns the name of the container identified by (@code this}.
   *
   * @return a container name
   */
  public String getContainerName() {
    return containerName;
  }

  public int totalSize() {
    return ((getRootPathName().length() + getContainerName().length()) * CHAR_SIZE) + INT_SIZE * 2;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    FrsContainerIdentifier that = (FrsContainerIdentifier)o;

    return rootPathName.equals(that.rootPathName) && containerName.equals(that.containerName);
  }

  @Override
  public int hashCode() {
    int result = rootPathName.hashCode();
    result = 31 * result + containerName.hashCode();
    return result;
  }

  ByteBuffer encode() {
    final StructEncoder<Void> encoder = STRUCT.encoder();
    encode(encoder);
    ByteBuffer byteBuffer = encoder.encode();
    byteBuffer.flip();
    return byteBuffer;
  }

  public void encode(PrimitiveEncodingSupport<?> structEncoder) {
    structEncoder.string(ROOT_PATH_NAME, rootPathName)
      .string(CONTAINER_NAME, containerName);
  }

  static FrsContainerIdentifier decode(ByteBuffer byteBuffer) {
    final StructDecoder<Void> decoder = STRUCT.decoder(byteBuffer);
    return decode(decoder);
  }

  public static FrsContainerIdentifier decode(PrimitiveDecodingSupport structDecoder) {
    return new FrsContainerIdentifier(structDecoder.string(ROOT_PATH_NAME), structDecoder.string(CONTAINER_NAME));
  }
}
