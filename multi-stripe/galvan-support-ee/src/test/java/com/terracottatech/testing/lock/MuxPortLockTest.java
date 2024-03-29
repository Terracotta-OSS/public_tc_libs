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
package com.terracottatech.testing.lock;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class MuxPortLockTest {
  @Mock
  private PortLock lock1;

  @Mock
  private PortLock lock2;

  @Test
  public void getPort() {
    MuxPortLock lock = new MuxPortLock(1);
    assertEquals(1, lock.getPort());
  }

  @Test
  public void closeEmpty() {
    MuxPortLock lock = new MuxPortLock(1);

    lock.addPortLock(lock1);
    lock.addPortLock(lock2);

    lock.close();

    verify(lock1).close();
    verify(lock2).close();
  }

  @Test
  public void closeWithThrowing() {
    doThrow(PortLockingException.class).when(lock1).close();
    doThrow(PortLockingException.class).when(lock2).close();

    MuxPortLock lock = new MuxPortLock(1);

    lock.addPortLock(lock1);
    lock.addPortLock(lock2);

    try {
      lock.close();
      fail("Expected PortLockingException");
    } catch (PortLockingException e) {
      // Expected
    }

    verify(lock1).close();
    verify(lock2).close();
  }
}
