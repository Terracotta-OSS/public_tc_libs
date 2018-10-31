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
package com.terracottatech.store.common.messages;

import com.terracottatech.store.common.exceptions.ReflectiveExceptionBuilder;
import org.terracotta.runnel.Struct;
import org.terracotta.runnel.StructBuilder;
import org.terracotta.runnel.decoding.StructArrayDecoder;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.StructArrayEncoder;
import org.terracotta.runnel.encoding.StructEncoder;

public class ThrowableCodec {
  protected static final int LAST_INDEX = 50;

  protected static final Struct STE_STRUCT = StructBuilder.newStructBuilder()
      .string("declaringClass", 10)
      .string("methodName", 20)
      .string("fileName", 30)
      .int32("lineNumber", 40)
      .build();

  protected final StructBuilder throwableStructBuilder;
  private final Struct throwableStruct;

  public ThrowableCodec() {
    throwableStructBuilder = StructBuilder.newStructBuilder();
    throwableStruct = throwableStructBuilder.alias();

    throwableStructBuilder
        .string("fqcn", 10)
        .string("message", 20)
        .struct("cause", 30, throwableStruct)
        .structs("stackTraceElements", 40, STE_STRUCT)
        .structs("suppressed", LAST_INDEX, throwableStruct)
        .build();
  }

  public Struct getThrowableStruct() {
    return throwableStruct;
  }

  public void encode(StructEncoder<?> encoder, Throwable t) {
    encoder.string("fqcn", t.getClass().getName());
    encoder.string("message", t.getMessage());
    Throwable cause = t.getCause();
    if (cause != null) {
      encoder.struct("cause", cause, this::encode);
    }
    encodeStackTraceElements(t, encoder.structs("stackTraceElements"));
    encoder.structs("suppressed", t.getSuppressed(), this::encode);
  }

  protected static void encodeStackTraceElements(Throwable exception, StructArrayEncoder<?> arrayEncoder) {
    for (StackTraceElement stackTraceElement : exception.getStackTrace()) {
      StructEncoder<?> element = arrayEncoder.add();
      element.string("declaringClass", stackTraceElement.getClassName());
      element.string("methodName", stackTraceElement.getMethodName());
      if (stackTraceElement.getFileName() != null) {
        element.string("fileName", stackTraceElement.getFileName());
      }
      element.int32("lineNumber", stackTraceElement.getLineNumber());
      element.end();
    }
    arrayEncoder.end();
  }

  public Throwable decode(StructDecoder<?> decoder) {
    String exceptionClassName = decoder.string("fqcn");
    String message = decoder.string("message");

    StructDecoder<?> causeDecoder = decoder.struct("cause");
    Throwable t = rebuildThrowable(exceptionClassName, message, causeDecoder);

    StructArrayDecoder<?> stesArrayDecoder = decoder.structs("stackTraceElements");
    t.setStackTrace(decodeStackTraceElements(stesArrayDecoder));

    StructArrayDecoder<?> suppressedArrayDecoder = decoder.structs("suppressed");
    for (int i = 0; i < suppressedArrayDecoder.length(); i++) {
      Throwable suppressed = decode(suppressedArrayDecoder.next());
      t.addSuppressed(suppressed);
    }

    return t;
  }

  protected static StackTraceElement[] decodeStackTraceElements(StructArrayDecoder<?> arrayDecoder) {
    StackTraceElement[] stackTraceElements = new StackTraceElement[arrayDecoder.length()];
    for (int i = 0; i < arrayDecoder.length(); i++) {
      StructDecoder<?> element = arrayDecoder.next();
      stackTraceElements[i] = new StackTraceElement(
          element.string("declaringClass"),
          element.string("methodName"),
          element.string("fileName"),
          element.int32("lineNumber"));
      element.end();
    }
    arrayDecoder.end();
    return stackTraceElements;
  }

  protected Throwable rebuildThrowable(String throwableClassName, String message, StructDecoder<?> causeDecoder) {
    Throwable cause = null;
    if (causeDecoder != null) {
      cause = decode(causeDecoder);
    }

    return ReflectiveExceptionBuilder.buildThrowable(throwableClassName, message, cause);
  }
}
