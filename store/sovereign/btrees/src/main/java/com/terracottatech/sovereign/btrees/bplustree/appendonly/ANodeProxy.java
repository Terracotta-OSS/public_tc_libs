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

import com.terracottatech.sovereign.common.dumbstruct.Accessor;
import com.terracottatech.sovereign.common.dumbstruct.StructBuilder;
import com.terracottatech.sovereign.common.dumbstruct.fields.Int16Field;
import com.terracottatech.sovereign.common.dumbstruct.fields.Int64Field;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * This is the proxy class for reading and modifying the node information as it is
 * held in a DataBuffer. All raw manipulation is here, so the ANode class can deal with
 * logical operations. Note the use of two kinds of proxies to represent leaf and branch nodes.
 * <p>Layout is done with a 16 bit header, which contains a flag for leaf/branch, plus a 15-bit node key size.</p>
 * <p>Then for a leaf node, there are size*2 longs, laid out as interleaved keys/values. So key[0] is the
 * first long entry in the array, value[0] is the second, key[1] is the third, etc.</p>
 * <p>For a branch node, there are size*2+1 longs, laid out as interleaved pointers/keys,
 * plus an extra pointer at the end. So pointer[0] is the first long entry in the array,
 * key[0] is the second, pointer[1] is the third, etc.</p>
 * <p>Note that this means branch nodes ar 8 bytes larger than leaf nodes.</p>
 * @author cschanck
 */
public class ANodeProxy {
  private static final int PRECOMPUTED_SIZES = 500;
  private static final Int16Field HEADER_FIELD;
  private static final Int64Field REVISION_FIELD;
  private static final Int64Field ARRAY_FIELD;
  private static final ANodeProxy LEAF = new ANodeProxy(true);
  private static final ANodeProxy BRANCH = new ANodeProxy(false);
  private static final int[] LEAF_SIZES = new int[PRECOMPUTED_SIZES];
  private static final int[] BRANCH_SIZES = new int[PRECOMPUTED_SIZES];

  private final boolean isLeaf;

  /*
   * Build the struct, and precompute ndoe sizes.
   */
  static {
    StructBuilder b = new StructBuilder();
    HEADER_FIELD = b.int16();
    REVISION_FIELD = b.int64();
    ARRAY_FIELD = b.int64();
    b.end();
    for (int i = 0; i < PRECOMPUTED_SIZES; i++) {
      LEAF_SIZES[i] = sizeFor(true, i);
      BRANCH_SIZES[i] = sizeFor(false, i);
    }
  }

  /**
   * Instantiates a new A node storage.
   *
   * @param isLeaf the is leaf
   */
  public ANodeProxy(boolean isLeaf) {
    this.isLeaf = isLeaf;
  }

  /**
   * Return either a leaf or branch roxy, based on the header.
   * @param a Accessor
   * @return node proxy
   */
  public static ANodeProxy storageFor(Accessor a) {
    if (fetchLeaf(a)) {
      return LEAF;
    }
    return BRANCH;
  }

  /**
   * Canonical leaf proxy.
   * @return proxy
   */
  public static ANodeProxy leaf() {
    return LEAF;
  }

  /**
   * Canonical branch proxy.
   * @return proxy
   */
  public static ANodeProxy branch() {
    return BRANCH;
  }

  static boolean fetchLeaf(Accessor a) {
    return (HEADER_FIELD.get(a) >>> 15) != 0;
  }

  /**
   * Gets size.
   *
   * @param a the a
   * @return the size
   */
  public int getSize(Accessor a) {
    return HEADER_FIELD.get(a) & 0x7fff;
  }

  /**
   * Sets header.
   *
   * @param a  the a
   * @param sz the sz
   */
  public void setHeader(Accessor a, int sz) {
    short hdr = ((short) ((short) ((isLeaf ? 1 : 0) << 15) | (short) sz));
    HEADER_FIELD.put(a, hdr);
  }

  private int keyIndex(int keyIndex) {
    return 2 * keyIndex + (isLeaf ? 0 : 1);
  }

  private int pointerIndex(int pointerIndex) {
    return 2 * pointerIndex;
  }

  private int valueIndex(int valueIndex) {
    return 2 * valueIndex + 1;
  }

  /**
   * Gets key.
   *
   * @param a     the accessor
   * @param index the index of the key
   * @return the key
   */
  public long getKey(Accessor a, int index) {
    return ARRAY_FIELD.get(a, keyIndex(index));
  }

  /**
   * Sets key.
   *
   * @param a     the aaccessor
   * @param index the index of the key
   * @param val   the value to set
   */
  public void setKey(Accessor a, int index, long val) {
    ARRAY_FIELD.put(a, keyIndex(index), val);
  }

  /**
   * Sets value.
   *
   * @param a     the aaccessor
   * @param index the index of the value
   * @param val   the value to set
   */
  public void setValue(Accessor a, int index, long val) {
    ARRAY_FIELD.put(a, valueIndex(index), val);
  }

  /**
   * Gets pointer.
   *
   * @param a     the accessor
   * @param index the index of the pointer
   * @return the pointer value
   */
  public long getPointer(Accessor a, int index) {
    return ARRAY_FIELD.get(a, pointerIndex(index));
  }

  /**
   * Gets value.
   *
   * @param a     the accessor
   * @param index the index of the value
   * @return the value at that index
   */
  public long getValue(Accessor a, int index) {
    return ARRAY_FIELD.get(a, valueIndex(index));
  }

  /**
   * Sets pointer.
   *
   * @param a     the accessor
   * @param index the index of the pointer
   * @param val   the value of the pointer
   */
  public void setPointer(Accessor a, int index, long val) {
    int off = pointerIndex(index);
    ARRAY_FIELD.put(a, off, val);
  }

  /**
   * Insert a pointer/key pair at the specified pointer index, copying
   * as needed to make room.
   * @param a the accessor
   * @param index index of the pointer to insert atr
   * @param pointer pointer value
   * @param key key value
   * @param currentSize current size of the node
   */
  public void insertPointerKeyAt(Accessor a, int index, long pointer, long key, int currentSize) {
    int m = (currentSize - index) * 2 + 1;
    ARRAY_FIELD.move(a, pointerIndex(index), pointerIndex(index + 1), m);
    setPointer(a, index, pointer);
    setKey(a, index, key);
  }

  /**
   * Insert a key/pointer pair at the specified key index, copying
   * as needed to make room.
   * @param a the accessor
   * @param index index of the pointer to insert atr
   * @param key key value
   * @param pointer pointer value
   * @param currentSize current size of the node
   */
  public void insertKeyPointerAt(Accessor a, int index, long key, long pointer, int currentSize) {
    int m = (currentSize - index) * 2;
    ARRAY_FIELD.move(a, keyIndex(index), keyIndex(index + 1), m);
    setKey(a, index, key);
    setPointer(a, index + 1, pointer);
  }

  /**
   * Insert a key/value pair at the specified key index, copying
   * as needed to make room.
   * @param a the accessor
   * @param index index of the pointer to insert atr
   * @param key key value
   * @param value value
   * @param currentSize current size of the node
   */
  public void insertKeyValueAt(Accessor a, int index, long key, long value, int currentSize) {
    int m = (currentSize - index) * 2;
    ARRAY_FIELD.move(a, keyIndex(index), keyIndex(index + 1), m);
    setKey(a, index, key);
    setValue(a, index, value);
  }

  /**
   * Is this a leaf node?
   * @return
   */
  public boolean isLeaf() {
    return isLeaf;
  }

  /**
   * Get the revision.
   *
   * @param a the a
   * @return the revision
   */
  public long getRevision(Accessor a) {
    return REVISION_FIELD.get(a);
  }

  /**
   * Set the revision.
   *
   * @param a   the a
   * @param rev the rev
   */
  public void setRevision(Accessor a, long rev) {
    REVISION_FIELD.put(a, rev);
  }

  /**
   * Calculate the size in bytes for this type of node.
   * @param currentSize size of node
   * @return size in bytes
   */
  public int sizeFor(int currentSize) {
    if (currentSize < PRECOMPUTED_SIZES) {
      return isLeaf ? LEAF_SIZES[currentSize] : BRANCH_SIZES[currentSize];
    }
    return sizeFor(isLeaf, currentSize);
  }

  /**
   * Calculate the size in bytes for specified type of node
   * @param isLeaf is the node a leaf
   * @param currentSize size of node
   * @return size in bytes
   */
  private static int sizeFor(boolean isLeaf, int currentSize) {
    int sz = HEADER_FIELD.getAllocatedSize() + REVISION_FIELD.getAllocatedSize();
    sz = sz + ARRAY_FIELD.getSingleFieldSize() * 2 * currentSize;
    if (!isLeaf) {
      sz = sz + ARRAY_FIELD.getSingleFieldSize();
    }
    return sz;
  }

  public String toString(Accessor a, int size) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    pw.print("Node@" + size + "/" + getRevision(a) + "-" + (isLeaf ? "L" : "B"));
    pw.print(" Keys [");
    for (int i = 0; i < size; i++) {
      pw.print(getKey(a, i) + ", ");
    }
    pw.print("] ");
    if (isLeaf()) {
      pw.print("Vals [");
      for (int i = 0; i < size; i++) {
        pw.print(getValue(a, i) + ", ");
      }
      pw.print("]");
    } else {
      pw.print("Pointers [");
      for (int i = 0; i < size + 1; i++) {
        pw.print(getPointer(a, i) + ", ");
      }
      pw.print("]");
    }
    pw.close();
    return sw.toString();
  }

  /**
   * Remove key/value pair starting at specified key index, copying down to fill in the hole.
   * @param accessor accessor to use
   * @param index index of key
   * @param currentSize size of node
   */
  public void removeKeyValueAt(Accessor accessor, int index, int currentSize) {
    int m = (currentSize - index - 1) * 2;
    ARRAY_FIELD.move(accessor, keyIndex(index + 1), keyIndex(index), m);
  }

  /**
   * Remove key/pointer pair starting at specified key index, copying down to fill in the hole.
   * @param accessor accessor to use
   * @param index index of key
   * @param currentSize size of node
   */
  public void removeKeyPointerAt(Accessor accessor, int index, int currentSize) {
    int m = (currentSize - index - 1) * 2;
    ARRAY_FIELD.move(accessor, keyIndex(index + 1), keyIndex(index), m);
  }

  /**
   * Remove pointer/value pair starting at specified pointer index, copying down to fill in the hole.
   * @param accessor accessor to use
   * @param index index of pointer
   * @param currentSize size of node
   */
  public void removePointerKeyAt(Accessor accessor, int index, int currentSize) {
    int m = (currentSize - index - 1) * 2 + 1;
    ARRAY_FIELD.move(accessor, pointerIndex(index + 1), pointerIndex(index), m);
  }

  /**
   * Split copy from one node to the other.
   * @param splitPoint pointe to split at
   * @param a1 full node
   * @param a2 dest node
   * @param currentSize size of full node
   */
  public void splitCopy(int splitPoint, Accessor a1, Accessor a2, int currentSize) {
    if (isLeaf) {
      // woo woo pointer math
      a2.getDataBuffer().put(ARRAY_FIELD.getOffsetWithinStruct(), a1.getDataBuffer(),
        ARRAY_FIELD.getOffsetWithinStruct() + keyIndex(splitPoint) * ARRAY_FIELD.getSingleFieldSize(),
        (currentSize - splitPoint) * 2 * ARRAY_FIELD.getSingleFieldSize());
    } else {
      a2.getDataBuffer().put(ARRAY_FIELD.getOffsetWithinStruct(), a1.getDataBuffer(),
        ARRAY_FIELD.getOffsetWithinStruct() + pointerIndex(splitPoint + 1) * ARRAY_FIELD.getSingleFieldSize(),
        (currentSize - splitPoint - 1) * 2 * ARRAY_FIELD.getSingleFieldSize() + ARRAY_FIELD.getSingleFieldSize());
    }
  }
}
