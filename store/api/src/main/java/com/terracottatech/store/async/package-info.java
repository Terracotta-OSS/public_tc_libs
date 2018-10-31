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

/**
 * Contains types for performing asynchronous operations on datasets.
 * <p>
 *   Asynchronous operations can be performed on a {@link com.terracottatech.store.Dataset} by accessing an
 *   {@link com.terracottatech.store.async.AsyncDatasetReader} or
 *   {@link com.terracottatech.store.async.AsyncDatasetWriterReader} via an existing synchronous equivalent.  Via these
 *   asynchronous classes you can execute asynchronous CRUD operations and access a complete asynchronous equivalent of
 *   the JDK Streams API.  Asynchronous executions are represented through the returned
 *   {@link com.terracottatech.store.async.Operation} instances.  {@code Operation} extends both
 *   {@link java.util.concurrent.Future} and {@link java.util.concurrent.CompletionStage}, and so allows both interfacing
 *   with synchronous code, and the building of more complicated composite operations.
 */
package com.terracottatech.store.async;
