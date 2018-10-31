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
package com.terracottatech.connection.disconnect;

import com.tc.util.Assert;
import org.terracotta.entity.EndpointDelegate;
import org.terracotta.entity.EntityResponse;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An EndpointDelegate that can proxy another EndpointDelegate and fire an event when didDisconnectUnexpectedly() is
 * called.
 */
public class DisconnectDetectingDelegate<R extends EntityResponse> implements EndpointDelegate<R>, Closeable {
  private final DisconnectListener disconnectListener;
  private AtomicReference<EndpointDelegateState<R>> stateReference;

  public DisconnectDetectingDelegate(DisconnectListener disconnectListener) {
    this.disconnectListener = disconnectListener;
    this.stateReference = new AtomicReference<EndpointDelegateState<R>>(new EndpointDelegateState<R>());
  }

  public void setUnderlying(EndpointDelegate<R> delegate) {
    while (true) {
      EndpointDelegateState<R> state = stateReference.get();

      if (state.isClosed()) {
        throw new IllegalStateException("Endpoint closed");
      }

      Assert.assertNull(state.getDelegate());

      EndpointDelegateState<R> newState = state.setDelegate(delegate);
      boolean updated = stateReference.compareAndSet(state, newState);
      if (updated) {
        break;
      }
    }
  }

  @Override
  public void handleMessage(R entityResponse) {
    EndpointDelegate<R> delegate = stateReference.get().getDelegate();

    if (delegate != null) {
      delegate.handleMessage(entityResponse);
    }
  }

  @Override
  public byte[] createExtendedReconnectData() {
    EndpointDelegate<R> delegate = stateReference.get().getDelegate();

    if (delegate == null) {
      return new byte[0];
    }

    return delegate.createExtendedReconnectData();
  }

  @Override
  public void didDisconnectUnexpectedly() {
    EndpointDelegate<R> delegate = stateReference.get().getDelegate();

    if (delegate != null) {
      delegate.didDisconnectUnexpectedly();
    }

    disconnectListener.disconnected();
  }

  @Override
  public void close() {
    while (true) {
      EndpointDelegateState<R> state = stateReference.get();

      if (state.isClosed()) {
        break;
      }

      EndpointDelegateState<R> newState = state.close();

      boolean updated = stateReference.compareAndSet(state, newState);
      if (updated) {
        break;
      }
    }
  }

  private static class EndpointDelegateState<R extends EntityResponse> {
    private final boolean closed;
    private final EndpointDelegate<R> delegate;

    public EndpointDelegateState() {
      this(false, null);
    }

    EndpointDelegateState(boolean closed, EndpointDelegate<R> delegate) {
      this.closed = closed;
      this.delegate = delegate;
    }

    public boolean isClosed() {
      return closed;
    }

    public EndpointDelegate<R> getDelegate() {
      return delegate;
    }

    public EndpointDelegateState<R> setDelegate(EndpointDelegate<R> delegate) {
      return new EndpointDelegateState<R>(closed, delegate);
    }

    public EndpointDelegateState<R> close() {
      return new EndpointDelegateState<R>(true, delegate);
    }
  }
}
