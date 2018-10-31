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
package com.terracottatech.store.common.messages.indexing;

import org.terracotta.runnel.Struct;
import org.terracotta.runnel.decoding.Enm;

import com.terracottatech.store.common.messages.DatasetEntityResponse;
import com.terracottatech.store.common.messages.DatasetEntityResponseType;
import com.terracottatech.store.common.messages.DatasetStructBuilder;
import com.terracottatech.store.common.messages.DatasetStructDecoder;
import com.terracottatech.store.common.messages.DatasetStructEncoder;
import com.terracottatech.store.indexing.Index;

import static java.util.Objects.requireNonNull;

/**
 * The response to {@link IndexCreateStatusMessage}.
 */
public class IndexCreateStatusResponse extends DatasetEntityResponse {

  /**
   * The index status at completion of index creation; {@code null} if index creation failed.
   */
  private final Index.Status status;

  /**
   * The cause underlying an index creation failure; {@code null} if index creation was successful.
   */
  private final Throwable fault;

  /**
   * Retry the index create status as the failover happened in middle of process
   */
  private final boolean retry;

  /**
   * Creates a {@code IndexCreateStatusResponse} for a successfully created index.
   * @param status the current status of the index
   */
  public IndexCreateStatusResponse(Index.Status status) {
    this.status = requireNonNull(status, "status");
    this.fault = null;
    this.retry = false;
  }

  /**
   * Creates a {@code IndexCreateStatusResponse} for a failed creation request.
   * @param fault the index creation failure reson
   */
  public IndexCreateStatusResponse(Throwable fault) {
    this.status = null;
    this.fault = requireNonNull(fault, "fault");
    this.retry = false;
  }

  public IndexCreateStatusResponse(boolean retry) {
    this.status = null;
    this.fault = null;
    this.retry = retry;
  }

  @Override
  public DatasetEntityResponseType getType() {
    return DatasetEntityResponseType.INDEX_CREATE_STATUS_RESPONSE;
  }

  /**
   * Gets the status of the index at completion of index creation.
   * @return {@code null} if index creation failed; otherwise, the index status at index creation completion
   */
  public Index.Status getStatus() {
    return status;
  }

  /**
   * Gets the cause of indexing failure, if any.
   * @return {@code null} if the indexing operation was successful; otherwise, the failure cause if
   *    index creation failed
   */
  public Throwable getFault() {
    return fault;
  }

  public boolean isRetry() {
    return retry;
  }

  public static Struct struct(DatasetStructBuilder datasetStructBuilder) {
    datasetStructBuilder
        .enm("status", 10, IndexingCodec.INDEX_STATUS_ENUM_MAPPING)
        .throwable("fault", 20)
        .bool("retry", 30);
    return datasetStructBuilder.build();
  }

  public static void encode(DatasetStructEncoder encoder, DatasetEntityResponse message) {
    IndexCreateStatusResponse createResponse = (IndexCreateStatusResponse)message;
    Index.Status status = createResponse.getStatus();
    if (status != null) {
      encoder.enm("status", status);
    }
    Throwable fault = createResponse.getFault();
    if (fault != null) {
      encoder.throwable("fault", fault);
    }
    encoder.bool("retry", createResponse.isRetry());
  }

  public static IndexCreateStatusResponse decode(DatasetStructDecoder decoder) {
    IndexCreateStatusResponse createResponse = null;
    Enm<Index.Status> statusEnm = decoder.enm("status");
    if (statusEnm.isFound()) {
      createResponse = new IndexCreateStatusResponse(statusEnm.get());
    }
    Throwable fault = decoder.throwable("fault");
    if (fault != null) {
      if (createResponse != null) {
        throw new IllegalStateException("Attempt to decode IndexCreateStatusResponse with both status & fault set");
      }
      createResponse = new IndexCreateStatusResponse(fault);
    }
    boolean isRetry = decoder.bool("retry");
    if (isRetry) {
      if (createResponse != null) {
        throw new IllegalStateException("Attempt to decode IndexCreateStatusResponse with both status or fault when retry is set");
      }
      createResponse = new IndexCreateStatusResponse(isRetry);
    }
    if (createResponse == null) {
      throw new IllegalStateException("Attempt to decode IndexCreateStatusResponse with neither status & fault set");
    }
    return createResponse;
  }

}
