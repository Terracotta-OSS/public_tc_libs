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
package com.terracottatech.sovereign.btrees.bplustree.model;

/**
 * By providing a {@link com.terracottatech.sovereign.btrees.bplustree.model.TxDecoratorFactory} to a tree, each created
 * transaction will get a decorator created with it, along with the lifecycle calls outlined below. This allows for
 * tx-specific functionality to be bound to the tree.
 *
 * @author cschanck
 */
public interface TxDecorator {

  public void preCommitHook();

  public void postCommitHook();

  public void postBeginHook();

  public void preCloseHook();

  public void postCloseHook();

  public static class NoOp implements TxDecorator {

    @Override
    public void preCommitHook() {
      // noop
    }

    @Override
    public void postCommitHook() {
      // noop
    }

    @Override
    public void postBeginHook() {
      // noop
    }

    @Override
    public void preCloseHook() {
      // noop
    }

    @Override
    public void postCloseHook() {
      // noop
    }
  }
}
