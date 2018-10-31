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

/**
 *
 * @author mscott
 */
public class ExtractorBtreeMapsTest {
  // TODO XXX
//  private BtreeIndexMap<String> map;
//  private HashMap<Long, Object> srcmap = new HashMap<>();
//
//  @Mock
//  private MemoryContainer<Record<?>> container;
//
//  @Mock
//  private Context context;
//
//  @Mock
//  private IndexMapConfig<String> config;
//
//  public ExtractorBtreeMapsTest() {
//  }
//
//  @BeforeClass
//  public static void setUpClass() {
//  }
//
//  @AfterClass
//  public static void tearDownClass() {
//  }
//
//  @Before
//  public void setUp() {
//    MockitoAnnotations.initMocks(this);
//
//    PageSource pageSource = new UpfrontAllocatingPageSource(new OffHeapBufferSource(), 1024 * 1024, 512 * 1024 * 1024);
//    CellDefinition<String> def = CellDefinition.define("test",com.terracottatech.store.Type.STRING);
//    MemoryAddressList list = mock(MemoryAddressList.class);
//    when(list.get(Matchers.anyInt())).then(new Answer<Long>() {
//      @Override
//      public Long answer(InvocationOnMock invocation) throws Throwable {
//        return ((Integer)invocation.getArguments()[0]).longValue();
//      }
//    });
//    when(list.contains(Matchers.anyInt())).thenReturn(Boolean.TRUE);
//    when(config.getType()).thenReturn(Type.STRING);
//    when(config.isSortedMap()).thenReturn(true);
//    when(config.getContainer()).thenReturn(container);
//    when(container.createLocator(Matchers.anyLong(), Matchers.any())).then(new Answer<Locator>() {
//      @Override
//      public Locator answer(InvocationOnMock invocation) throws Throwable {
//        return new PersistentMemoryLocator(list, ((Long)invocation.getArguments()[0]).intValue(), (MemoryLocatorFactory)invocation.getArguments()[1]);
//      }
//    });
//    when(container.mapLocator(Matchers.any(PersistentMemoryLocator.class))).then(new Answer<Long>() {
//      @Override
//      public Long answer(InvocationOnMock invocation) throws Throwable {
//        return (long)((PersistentMemoryLocator)invocation.getArguments()[0]).index();
//      }
//    });
//    when(container.get(Matchers.any(Locator.class))).then(new Answer<Record>() {
//      @Override
//      public Record answer(InvocationOnMock outer) throws Throwable {
//        Record r = mock(Record.class);
//        when(r.get(Matchers.eq(def))).then(new Answer<Object>() {
//          @Override
//          public Object answer(InvocationOnMock inner) throws Throwable {
//            return srcmap.get(((Locator)outer.getArguments()[0]));
//          }
//        });
//        return r;
//      }
//    });
//    when(container.direct(Matchers.anyLong())).then(new Answer<Record>() {
//      @Override
//      public Record answer(InvocationOnMock outer) throws Throwable {
//        Record r = mock(Record.class);
//        when(r.get(Matchers.eq(def))).then(new Answer<Object>() {
//          @Override
//          public Object answer(InvocationOnMock inner) throws Throwable {
//            return Optional.of(srcmap.get(((Long)outer.getArguments()[0])));
//          }
//        });
//        return r;
//      }
//    });
//    KeyMapper<String> mapper = new ExtractorKeyMapper<>(config,(r) -> r.get(def).get());
//    map = new BtreeIndexMap<>(String.class, mapper, container,
//        new PageSourceLocation(pageSource, 1024 * 1024, 512 * 1024 * 1024), 32);
//  }
//
//  @After
//  public void tearDown() {
//  }
//
//  @Test
//  public void testExtractorKeySet() {
//    String test = "thisisatest=none";
//    long pos = 1;
//    for (char t : test.toCharArray()) {
////      Locator l = container.create((long)pos++, null);
//      String c = new String(new char[] {t});
//      srcmap.put(pos, c);
//      map.put(context, c, container.createLocator(pos++, null));
//    }
//    Locator l = map.first(context);
//    int count = 0;
//    String last = "";
//    while(!l.isEndpoint()) {
//      String v = srcmap.get(((MemoryLocator)l).uid()).toString();
//      Assert.assertThat(v, org.hamcrest.Matchers.greaterThanOrEqualTo(last));
//      System.out.println(v);
//      last = v;
//      l = l.next();
//      count++;
//    }
//    Assert.assertEquals(test.length(), count);
//  }
}
