/**
 * Copyright 2011 Booz Allen Hamilton.
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership. Booz Allen Hamilton
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.bah.culvert.util;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import static com.bah.culvert.util.Exceptions.MultiRuntimeException;

public class MultiRuntimeExceptionTest {

  /**
   * Test Empty MultiRuntimeException
   * 
   */
  @Test
  public void testEmptyMultiRuntimeException() {
    List<Throwable> list = new ArrayList<Throwable>();
    Assert.assertNotNull(MultiRuntimeException(null));
    Assert.assertNotNull(MultiRuntimeException(list));
  }

  /**
   * Test MultiRuntimeException
   * 
   */
  @Test
  public void testMultiRuntimeException() {
    List<Throwable> list = new ArrayList<Throwable>();
    list.add(new RuntimeException("This is test exception 1"));
    list.add(new RuntimeException("This is test exception 2"));
    MultiRuntimeException mre = MultiRuntimeException(list);
    Assert.assertNotNull(mre.getMessage());
  }

}
