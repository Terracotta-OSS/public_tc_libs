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
package com.terracottatech.sovereign.impl.memory;

import com.terracottatech.sovereign.spi.store.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author cschanck
 **/
public class ContextImpl implements Context {

  private static final Logger LOGGER = LoggerFactory.getLogger(MemoryRecordContainer.class);
  private final State state;

  static class State implements AutoCloseable {
    private final boolean guard;
    private AbstractRecordContainer<?> cont;
    private volatile boolean open = true;
    private final Set<AutoCloseable> closeSet = new HashSet<>();
    private Throwable allocationStackTrace;

    public State(ContextImpl impl, boolean guard, AbstractRecordContainer<?> cont, Throwable allocationStackTrace) {
      this.cont = cont;
      this.guard = guard;
      this.allocationStackTrace = allocationStackTrace;
      if (guard) {
        cont.getContextFinalizer().record(impl, this);
      }
    }

    public void setOpen(boolean open) {
      this.open = open;
    }

    public boolean isOpen() {
      return open;
    }

    public Set<AutoCloseable> getCloseSet() {
      return closeSet;
    }

    public Throwable getAllocationStackTrace() {
      return allocationStackTrace;
    }

    public void close() {
      if (open) {
        open = false;
        if (guard) {
          cont.getContextFinalizer().remove(this);
        }
        final AtomicReference<Exception> firstException = new AtomicReference<>();
        closeSet.forEach(c -> {
          try {
            c.close();
          } catch (Exception e) {
            // Save exceptions thrown by closeSet methods; first one is rethrown later
            if (!firstException.compareAndSet(null, e)) {
              firstException.get().addSuppressed(e);
            }
          }
        });

        Exception closeException = firstException.get();
        if (closeException != null) {
          if (closeException instanceof RuntimeException) {
            throw (RuntimeException) closeException;
          }
          throw new RuntimeException(closeException);
        }
      }
    }
  }

  private static final boolean CAPTURE_CONTEXT_ALLOCATION_TRACE = Boolean.valueOf(
    System.getProperty("sovereign.capture_context_allocation_trace", "false"));

  public ContextImpl(AbstractRecordContainer<?> cont, boolean guard) {
    Throwable allocationStackTrace;
    if (CAPTURE_CONTEXT_ALLOCATION_TRACE) {
      allocationStackTrace = new Throwable("Context allocation");
    } else {
      allocationStackTrace = null;
    }
    this.state = new State(this, guard, cont, allocationStackTrace);
  }

  @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
  @Override
  public void close() {
    state.close();
  }

  @Override
  public void addCloseable(AutoCloseable c) {
    state.getCloseSet().add(c);
  }

  public long getStartingRevision() {
    throw new UnsupportedOperationException();
  }

  State getState() {
    return state;
  }

}
