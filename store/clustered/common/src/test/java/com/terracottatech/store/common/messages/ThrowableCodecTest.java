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

import com.terracottatech.store.common.exceptions.UnavailableException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtConstructor;
import org.junit.Test;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.StructEncoder;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;

/**
 * @author Ludovic Orban
 */
public class ThrowableCodecTest {
  @Test
  public void testEncodingDecodingNoThrowableCtorExceptionMessage() throws Exception {
    ThrowableCodec codec = new ThrowableCodec();
    StructEncoder<Void> encoder = codec.getThrowableStruct().encoder();

    Exception ex = new Exception("root cause");
    codec.encode(encoder, new NoCauseCtorException("boom!").initCause(ex));
    ByteBuffer encoded = encoder.encode();

    encoded.rewind();
    StructDecoder<Void> decoder = codec.getThrowableStruct().decoder(encoded);
    Throwable decoded = codec.decode(decoder);

    assertThat(decoded, instanceOf(NoCauseCtorException.class));
    assertThat(decoded.getMessage(), is("boom!"));
    assertThat(decoded.getCause(), instanceOf(Exception.class));
    assertThat(decoded.getCause().getMessage(), is("root cause"));
    assertThat(decoded.getCause().getCause(), is(nullValue()));
  }

  @Test
  public void testEncodingDecodingNoThrowableCtorExceptionNoMessage() throws Exception {
    ThrowableCodec codec = new ThrowableCodec();
    StructEncoder<Void> encoder = codec.getThrowableStruct().encoder();

    Exception ex = new Exception("root cause");
    codec.encode(encoder, new NoCauseCtorException().initCause(ex));
    ByteBuffer encoded = encoder.encode();

    encoded.rewind();
    StructDecoder<Void> decoder = codec.getThrowableStruct().decoder(encoded);
    Throwable decoded = codec.decode(decoder);

    assertThat(decoded, instanceOf(NoCauseCtorException.class));
    assertThat(decoded.getMessage(), is(nullValue()));
    assertThat(decoded.getCause(), instanceOf(Exception.class));
    assertThat(decoded.getCause().getMessage(), is("root cause"));
    assertThat(decoded.getCause().getCause(), is(nullValue()));
  }

  @Test
  public void testEncodingDecodingAssertionError() throws Exception {
    ThrowableCodec codec = new ThrowableCodec();
    StructEncoder<Void> encoder = codec.getThrowableStruct().encoder();

    codec.encode(encoder, new AssertionError());
    ByteBuffer encoded = encoder.encode();

    encoded.rewind();
    StructDecoder<Void> decoder = codec.getThrowableStruct().decoder(encoded);
    Throwable decoded = codec.decode(decoder);

    assertThat(decoded, instanceOf(AssertionError.class));
    assertThat(decoded.getMessage(), is(nullValue()));
    assertThat(decoded.getCause(), is(nullValue()));
  }

  @Test
  public void testEncodingDecodingAssertionErrorMsg() throws Exception {
    ThrowableCodec codec = new ThrowableCodec();
    StructEncoder<Void> encoder = codec.getThrowableStruct().encoder();

    codec.encode(encoder, new AssertionError("test assertion failed"));
    ByteBuffer encoded = encoder.encode();

    encoded.rewind();
    StructDecoder<Void> decoder = codec.getThrowableStruct().decoder(encoded);
    Throwable decoded = codec.decode(decoder);

    assertThat(decoded, instanceOf(AssertionError.class));
    assertThat(decoded.getMessage(), is("test assertion failed"));
    assertThat(decoded.getCause(), is(nullValue()));
  }

  @Test
  public void testEncodingDecodingAssertionErrorMsgCause() throws Exception {
    ThrowableCodec codec = new ThrowableCodec();
    StructEncoder<Void> encoder = codec.getThrowableStruct().encoder();

    Exception ex = new Exception("test exception");
    codec.encode(encoder, new AssertionError(ex));
    ByteBuffer encoded = encoder.encode();

    encoded.rewind();
    StructDecoder<Void> decoder = codec.getThrowableStruct().decoder(encoded);
    Throwable decoded = codec.decode(decoder);

    assertThat(decoded, instanceOf(AssertionError.class));
    assertThat(decoded.getMessage(), is("java.lang.Exception: test exception"));
    assertThat(decoded.getCause(), instanceOf(Exception.class));
    assertThat(decoded.getCause().getMessage(), is("test exception"));
    assertThat(decoded.getCause().getCause(), is(nullValue()));
  }

  @Test
  public void testEncodingDecodingExceptionChain() throws Exception {
    ThrowableCodec codec = new ThrowableCodec();
    StructEncoder<Void> encoder = codec.getThrowableStruct().encoder();

    Throwable ioe = new IOException("i/o error");
    ioe.addSuppressed(new IllegalStateException("suppressed 1"));
    ioe.addSuppressed(new IllegalArgumentException("suppressed 2"));
    RuntimeException re = new RuntimeException("runtime mess", ioe);
    re.addSuppressed(new UnsupportedOperationException("suppressed 3"));
    Exception ex = new Exception("I am too busy!", re);
    codec.encode(encoder, ex);
    ByteBuffer encoded = encoder.encode();

    encoded.rewind();
    StructDecoder<Void> decoder = codec.getThrowableStruct().decoder(encoded);
    Throwable decoded = codec.decode(decoder);

    assertThat(decoded, instanceOf(Exception.class));
    assertThat(decoded.getMessage(), is("I am too busy!"));

    assertThat(decoded.getCause(), instanceOf(RuntimeException.class));
    assertThat(decoded.getCause().getMessage(), is("runtime mess"));
    assertThat(decoded.getCause().getSuppressed().length, is(1));
    assertThat(decoded.getCause().getSuppressed()[0], instanceOf(UnsupportedOperationException.class));
    assertThat(decoded.getCause().getSuppressed()[0].getMessage(), is("suppressed 3"));

    assertThat(decoded.getCause().getCause(), instanceOf(IOException.class));
    assertThat(decoded.getCause().getCause().getMessage(), is("i/o error"));
    assertThat(decoded.getCause().getCause().getCause(), is(nullValue()));
    assertThat(decoded.getCause().getCause().getSuppressed().length, is(2));
    assertThat(decoded.getCause().getCause().getSuppressed()[0], instanceOf(IllegalStateException.class));
    assertThat(decoded.getCause().getCause().getSuppressed()[0].getMessage(), is("suppressed 1"));
    assertThat(decoded.getCause().getCause().getSuppressed()[1], instanceOf(IllegalArgumentException.class));
    assertThat(decoded.getCause().getCause().getSuppressed()[1].getMessage(), is("suppressed 2"));
  }

  @Test
  public void testWithUnknownClassMiddleCause() throws Exception {
    ThrowableCodec codec = new ThrowableCodec();
    StructEncoder<Void> encoder = codec.getThrowableStruct().encoder();

    Exception ex = newExceptionNotInClasspath("com.pany.TestException", "test message");
    IOException ioe = new IOException("io meltdown");
    ex.initCause(ioe);
    RuntimeException re = new RuntimeException("runtime problem", ex);
    codec.encode(encoder, re);
    ByteBuffer encoded = encoder.encode();

    encoded.rewind();
    Throwable decoded = codec.decode(codec.getThrowableStruct().decoder(encoded));

    assertThat(decoded, instanceOf(RuntimeException.class));
    assertThat(decoded.getMessage(), is("runtime problem"));

    assertThat(decoded.getCause(), instanceOf(UnavailableException.class));
    assertThat(decoded.getCause().getMessage(), is("[com.pany.TestException]: test message"));

    assertThat(decoded.getCause().getCause(), instanceOf(IOException.class));
    assertThat(decoded.getCause().getCause().getMessage(), is("io meltdown"));
    assertThat(decoded.getCause().getCause().getCause(), is(nullValue()));
  }

  @Test
  public void testWithUnknownClassRootCause() throws Exception {
    ThrowableCodec codec = new ThrowableCodec();
    StructEncoder<Void> encoder = codec.getThrowableStruct().encoder();

    Exception ex = newExceptionNotInClasspath("an.other.com.pany.TestException", "test message");
    RuntimeException re = new RuntimeException("runtime problem", ex);
    codec.encode(encoder, re);
    ByteBuffer encoded = encoder.encode();

    encoded.rewind();
    Throwable decoded = codec.decode(codec.getThrowableStruct().decoder(encoded));

    assertThat(decoded, instanceOf(RuntimeException.class));
    assertThat(decoded.getMessage(), is("runtime problem"));

    assertThat(decoded.getCause(), instanceOf(UnavailableException.class));
    assertThat(decoded.getCause().getMessage(), is("[an.other.com.pany.TestException]: test message"));
    assertThat(decoded.getCause().getCause(), is(nullValue()));
  }

  private Exception newExceptionNotInClasspath(String className, String message) throws Exception {
    ClassPool pool = ClassPool.getDefault();

    CtClass exceptionCtClass = pool.get("java.lang.Exception");
    CtClass newExceptionCtClass = pool.makeClass(className);

    newExceptionCtClass.setSuperclass(exceptionCtClass);
    CtConstructor ctConstructor = new CtConstructor(new CtClass[]{pool.get("java.lang.String")}, newExceptionCtClass);
    ctConstructor.setBody("super($1);");
    newExceptionCtClass.addConstructor(ctConstructor);

    ClassLoader classLoader = new ClassLoader() {};
    @SuppressWarnings("unchecked")
    Class<Exception> exceptionClass = newExceptionCtClass.toClass(classLoader, null);

    return exceptionClass.getConstructor(String.class).newInstance(message);
  }

}
