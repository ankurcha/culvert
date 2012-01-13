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
package com.bah.culvert.index.acceptor;

import com.bah.culvert.data.CKeyValue;
import com.bah.culvert.index.Index;
import com.bah.culvert.transactions.Put;
import com.bah.culvert.util.BaseConfigurable;

/**
 * Determine if a {@link CKeyValue} is accepted by an {@link Index} to be
 * indexed.
 */
public abstract class Acceptor extends BaseConfigurable {

  /**
   * Accept or reject a given key/value for indexing. After accepting a
   * {@link CKeyValue}, that object and all other accepted {@link CKeyValue
   * CKeyValues} will be passed as a {@link Put} to {@link Index#handlePut(Put)}
   * @param kv key/value to be indexed
   * @return <tt>true</tt> if the key/value should be passed to the index,
   *         <tt>false</tt> otherwise
   */
  public abstract boolean accept(CKeyValue kv);
  

  /**
   * Reject all key/values.
   * <p>
   * This is the default acceptor, if none can be found.
   */
  public static class AcceptNone extends Acceptor {

    @Override
    public boolean accept(CKeyValue kv) {
      return false;
    }

  }

}
