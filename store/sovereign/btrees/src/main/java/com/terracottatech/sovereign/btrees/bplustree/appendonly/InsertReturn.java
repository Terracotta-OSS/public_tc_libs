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
package com.terracottatech.sovereign.btrees.bplustree.appendonly;

import com.terracottatech.sovereign.btrees.bplustree.model.Node;

/**
 * The Insert return type, used when returning status about an insert operation.
 *
 * @author cschanck
 */
class InsertReturn {

  private boolean bubbled = false;

  private boolean nodeChanged = false;

  private long bubbleKey = 0;

  private Node bubblePrevNode = null;

  private Node bubbleNextNode = null;

  private boolean replaced = false;

  private Long valueInvolved = null;

  private Long keyRemoved = null;

  @Override
  public String toString() {
    return "InsertReturn{" +
      "bubbled=" + bubbled +
      ", bubbleKey=" + bubbleKey +
      ", bubblePrevNode=" + bubblePrevNode +
      ", bubbleNextNode=" + bubbleNextNode +
      ", replaced=" + replaced +
      ", keyRemoved=" + keyRemoved +
      ", valueInvolved=" + valueInvolved +
      '}';
  }

  public boolean isBubbled() {
    return bubbled;
  }

  public boolean isNodeChanged() {
    return nodeChanged;
  }

  /**
   * Gets bubble key.
   *
   * @return the bubble key
   */
  public long getBubbleKey() {
    return bubbleKey;
  }

  /**
   * Gets bubble prev node.
   *
   * @return the bubble prev node
   */
  public Node getBubblePrevNode() {
    return bubblePrevNode;
  }

  /**
   * Gets bubble next node.
   *
   * @return the bubble next node
   */
  public Node getBubbleNextNode() {
    return bubbleNextNode;
  }

  public boolean isReplaced() {
    return replaced;
  }

  /**
   * Gets involved value.
   *
   * @return the involved value
   */
  public Long getInvolvedValue() {
    return valueInvolved;
  }

  /**
   * Gets key removed.
   *
   * @return the key removed
   */
  public Long getKeyRemoved() {
    return keyRemoved;
  }

  /**
   * Sets bubbled.
   *
   * @param bubbled the bubbled
   * @return the bubbled
   */
  public InsertReturn setBubbled(boolean bubbled) {
    this.bubbled = bubbled;
    return this;
  }

  /**
   * Sets node changed.
   *
   * @param nodeChanged the node changed
   * @return the node changed
   */
  public InsertReturn setNodeChanged(boolean nodeChanged) {
    this.nodeChanged = nodeChanged;
    return this;
  }

  /**
   * Sets bubble key.
   *
   * @param bubbleKey the bubble key
   * @return the bubble key
   */
  public InsertReturn setBubbleKey(long bubbleKey) {
    this.bubbleKey = bubbleKey;
    return this;
  }

  /**
   * Sets bubble prev node.
   *
   * @param bubblePrevNode the bubble prev node
   * @return the bubble prev node
   */
  public InsertReturn setBubblePrevNode(Node bubblePrevNode) {
    this.bubblePrevNode = bubblePrevNode;
    return this;
  }

  /**
   * Sets bubble next node.
   *
   * @param bubbleNextNode the bubble next node
   * @return the bubble next node
   */
  public InsertReturn setBubbleNextNode(Node bubbleNextNode) {
    this.bubbleNextNode = bubbleNextNode;
    return this;
  }

  /**
   * Sets replaced.
   *
   * @param replaced the replaced
   * @return the replaced
   */
  public InsertReturn setReplaced(boolean replaced) {
    this.replaced = replaced;
    return this;
  }

  /**
   * Sets value involved.
   *
   * @param valueInvolved the value involved
   * @return the value involved
   */
  public InsertReturn setValueInvolved(Long valueInvolved) {
    this.valueInvolved = valueInvolved;
    return this;
  }

  /**
   * Sets key removed.
   *
   * @param keyRemoved the key removed
   * @return the key removed
   */
  public InsertReturn setKeyRemoved(Long keyRemoved) {
    this.keyRemoved = keyRemoved;
    return this;
  }

}

