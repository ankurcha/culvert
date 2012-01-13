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

import org.apache.hadoop.conf.Configuration;

import com.bah.culvert.data.CKeyValue;
import com.bah.culvert.util.Bytes;
import com.bah.culvert.util.ConfUtils;

/**
 * Acceptor that matches the {@link CKeyValue} against the stored column family
 * and column qualifier.
 * <p>
 * Only {@link CKeyValue CKeyValues} that match <b>both</b> the family and
 * qualifier will be accepted.
 * <p>
 * <b>NOTE:<b> This class is not thread-safe, and (re)configuration during use
 * will likely lead to undefined behavior.
 */
public class ColumnMatchingAcceptor extends Acceptor {

  private static final String COL_FAM_CONF_KEY = "culvert.index.acceptor.column.family";
  private static final String FAM_BASE64_ENCODED_CONF_KEY = "culvert.index.acceptor.column.family.base64";
  private static final String COL_QUAL_CONF_KEY = "culvert.index.acceptor.column.qualifier";
  private static final String QUAL_BASE64_ENCODED_CONF_KEY = "culvert.index.acceptor.column.qual.base64";

  private byte[] cf;
  private byte[] cq;

  /**
   * Match {@link CKeyValue} against the specified column family and column
   * qualifier.
   * <p>
   * Matches are done byte-wise, lexicographically
   * @param cf family to compare against (this is checked first)
   * @param cq qualifier to compare against (checked second)
   */
  public ColumnMatchingAcceptor(byte[] cf, byte[] cq) {
    Configuration conf = new Configuration(false);
    setColumn(cf, cq, conf);
    super.setConf(conf);
  }

  /**
   * Match {@link CKeyValue} against the specified column family and column
   * qualifier.
   * <p>
   * This must be configured via {@link #setConf(Configuration)} with the
   * expected values for the family and qualifier already stored in the
   * {@link Configuration} via {@link #setColumn(byte[], byte[], Configuration)}
   */
  public ColumnMatchingAcceptor() {
  }

  /**
   * Set the column family and qualifier that this acceptor should use when
   * determining if a row
   * @param cf to store
   * @param cq to store
   * @param conf to be updated. The same {@link Configuration} should be set via
   *        {@link #setConf(Configuration)}
   */
  public static void setColumn(byte[] cf, byte[] cq, Configuration conf) {
    ConfUtils.setBinaryConfSetting(FAM_BASE64_ENCODED_CONF_KEY,
        COL_FAM_CONF_KEY, cf, conf);
    ConfUtils.setBinaryConfSetting(QUAL_BASE64_ENCODED_CONF_KEY,
        COL_QUAL_CONF_KEY, cq, conf);
  }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
      cf = ConfUtils.getBinaryConfSetting(FAM_BASE64_ENCODED_CONF_KEY,
          COL_FAM_CONF_KEY, conf);
      cq = ConfUtils.getBinaryConfSetting(QUAL_BASE64_ENCODED_CONF_KEY,
          COL_QUAL_CONF_KEY, conf);
  }

  @Override
  public boolean accept(CKeyValue kv) {
      // actually do the comparison
      if (Bytes.compareTo(cf, kv.getFamily()) == 0)
        if (Bytes.compareTo(cq, kv.getQualifier()) == 0)
          return true;
    return false;
  }
}
