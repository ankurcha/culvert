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
package com.bah.culvert.index;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.mortbay.log.Log;

import com.bah.culvert.Client;
import com.bah.culvert.adapter.DatabaseAdapter;
import com.bah.culvert.adapter.TableAdapter;
import com.bah.culvert.data.CKeyValue;
import com.bah.culvert.data.CRange;
import com.bah.culvert.index.acceptor.Acceptor;
import com.bah.culvert.index.acceptor.ColumnMatchingAcceptor;
import com.bah.culvert.iterators.SeekingCurrentIterator;
import com.bah.culvert.transactions.Put;
import com.bah.culvert.util.BaseConfigurable;
import com.bah.culvert.util.Bytes;
import com.bah.culvert.util.ConfUtils;
import com.bah.culvert.util.LexicographicBytesComparator;
import com.google.common.base.Objects;

//import static com.bah.culvert.util.Constants.*;

/**
 * An index on a table. Index implementations may be instantiated multiple times
 * over a table or even a single column in a table. This promotes code reuse.
 * <p>
 * Indices are uniquely identified by their name. This promotes a functional
 * programming style so that multiple instantiations can logically refer to the
 * same index.
 */
public abstract class Index extends BaseConfigurable implements Writable {

  public static final Charset UTF_8 = Charset.forName("UTF-8");
  private static final String NAME_CONF_KEY = "culvert.index.name";
  private static final String PRIMARY_TABLE_CONF_KEY = "culvert.index.table.primary";
  private static final String INDEX_TABLE_CONF_KEY = "culvert.index.table.index";
  private static final String COL_FAM_CONF_KEY = "culvert.index.family";
  private static final String FAM_BASE64_ENCODED_CONF_KEY = "culvert.index.family.base64";
  private static final String COL_QUAL_CONF_KEY = "culvert.index.qualifier";
  private static final String QUAL_BASE64_ENCODED_CONF_KEY = "culvert.index.qual.base64";

  private static final String ACCEPTOR_CLASS_KEY = "culvert.index.acceptor.class";
  private static final String ACCEPTOR_CONF_KEY = "culvert.index.acceptor.conf";
  

  /**
   * For use with {@link #readFields(DataInput)}
   */
  public Index() {

  }

  /**
   * Create an index around the specified values
   * @param name of the index
   * @param columnFamily that this index indexes
   * @param columnQualifier that this index indexes
   * @param database that this database can access to
   * @param primaryTable
   * @param indexTable
   */
  @Deprecated
  public Index(String name, byte[] columnFamily, byte[] columnQualifier,
      DatabaseAdapter database, String primaryTable, String indexTable) {
    Configuration conf = new Configuration(false);
    super.setConf(conf);

    // Set the configuration
    Index.setIndexName(name, conf);
    Index.setColumnFamily(columnFamily, conf);
    Index.setColumnQualifier(columnQualifier, conf);
    Acceptor accept = new ColumnMatchingAcceptor(columnFamily, columnQualifier);
    Index.setAcceptor(accept, conf);
    Index.setPrimaryTable(primaryTable, conf);
    Index.setIndexTable(indexTable, conf);

    // Store the database
    Index.setDatabaseAdapter(database, conf);
  }

  public Index(String name, Acceptor acceptor, DatabaseAdapter database,
      String primaryTable, String indexTable) {
    // TODO implement constructor

  }

  public Index(String name, Class<? extends Acceptor> acceptor,
      DatabaseAdapter database, String primaryTable, String indexTable) {
    // TODO implement constructor
  }

  /**
   * Set the index name.
   * @param name The name of the index.
   * @param conf The configuration to set.
   */
  public static void setIndexName(String name, Configuration conf) {
    conf.set(NAME_CONF_KEY, name);
  }

  /**
   * Set the name of the data table containing the indexed row tuple.
   * @param table The name of the data table.
   * @param conf The configuration to set.
   */
  public static void setPrimaryTable(String table, Configuration conf) {
    conf.set(PRIMARY_TABLE_CONF_KEY, table);
  }

  /**
   * Set the name of the index table. This should refer to the actual name of
   * the index table. This is can be different than the name of the index.
   * @param table The name of the index table.
   * @param conf The configuration to set.
   */
  public static void setIndexTable(String table, Configuration conf) {
    conf.set(INDEX_TABLE_CONF_KEY, table);
  }

  /**
   * Set the column family of the column tuple that is being indexed.
   * @param colFam The column family.
   * @param conf The configuration to set.
   */
  public static void setColumnFamily(String colFam, Configuration conf) {
    conf.set(COL_FAM_CONF_KEY, colFam);
  }

  /**
   * Set the column qualifier of the column tuple that is being indexed.
   * @param colQual The column qualifier.
   * @param conf The configuration to set.
   */
  public static void setColumnQualifier(String colQual, Configuration conf) {
    conf.set(COL_QUAL_CONF_KEY, colQual);
  }

  /**
   * Set the acceptor the index should use
   * @param clazz {@link Acceptor} to store
   * @param acceptorConf {@link Configuration} for the acceptor
   * @param conf {@link Configuration} for the index in which to store the
   *        acceptor and its conf
   */
  public static void setAcceptor(Class<? extends Acceptor> clazz,
      Configuration acceptorConf, Configuration conf) {
    conf.setClass(ACCEPTOR_CLASS_KEY, clazz, Acceptor.class);
    ConfUtils.packConfigurationInPrefix(ACCEPTOR_CONF_KEY, acceptorConf, conf);
  }

  /**
   * Set the {@link Acceptor} the index should use. This {@link Acceptor} will
   * not be stored as-is, so it must be fully reinstantiated post a call to
   * {@link Acceptor#setConf(Configuration)}.
   * @param acceptor {@link Acceptor} to store. The {@link Acceptor Acceptor's}
   *        configuration will be extracted, stored, and reapplied on use.
   * @param conf {@link Configuration} for the index in which to store the
   *        acceptor and its conf
   */
  public static void setAcceptor(Acceptor acceptor, Configuration conf) {
    setAcceptor(acceptor.getClass(), acceptor.getConf(), conf);
  }

  /**
   * Set the column family (in bytes) of the column tuple that is being indexed.
   * @param colFam The column family.
   * @param conf The configuration to set.
   */
  public static void setColumnFamily(byte[] colFam, Configuration conf) {
    ConfUtils.setBinaryConfSetting(FAM_BASE64_ENCODED_CONF_KEY,
        COL_FAM_CONF_KEY, colFam,
        conf);
  }

  /**
   * Set the column qualifier (in bytes) of the column tuple that is being
   * indexed.
   * @param colQual The column qualifier.
   * @param conf The configuration to set.
   */
  public static void setColumnQualifier(byte[] colQual, Configuration conf) {
    ConfUtils.setBinaryConfSetting(QUAL_BASE64_ENCODED_CONF_KEY,
        COL_QUAL_CONF_KEY,
        colQual, conf);
  }

  public static String getPrimaryTableName(Configuration conf) {
    return conf.get(PRIMARY_TABLE_CONF_KEY);
  }

  /**
   * Set the database adapter that should be used with this index
   * @param database Configured database to store
   * @param conf Index's configuration to store the database in
   */
  public static void setDatabaseAdapter(DatabaseAdapter database,
      Configuration conf) {
    DatabaseAdapter.writeToConfiguration(database.getClass(),
        database.getConf(), conf);
  }

  /**
   * Set the database adapter that should be used with this index
   * @param dbClass database class that the index should use
   * @param dbConf configuration for the database, to use when it is
   *        instantiated
   * @param conf Index's configuration to store the database in
   */
  public static void setDatabaseAdapter(
      Class<? extends DatabaseAdapter> dbClass, Configuration dbConf,
      Configuration conf) {
    DatabaseAdapter.writeToConfiguration(dbClass, dbConf, conf);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    return Objects.hashCode(getName(), getColumnFamily(), getColumnQualifier(),
        getIndexTable());
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    String s = ("Name:" + getName());
    return s;
  }

  /**
   * Get the column family that this index is configured to index.
   * @return The column family that this index is configured to index.
   */
  public byte[] getColumnFamily() {
    return ConfUtils.getBinaryConfSetting(FAM_BASE64_ENCODED_CONF_KEY,
        COL_FAM_CONF_KEY, getConf());
  }

  /**
   * Get the column family that this index is configured to index.
   * @return The column family that this index is configured to index.
   */
  public byte[] getColumnQualifier() {
    return ConfUtils.getBinaryConfSetting(QUAL_BASE64_ENCODED_CONF_KEY,
        COL_QUAL_CONF_KEY, getConf());
  }

  /**
   * Get the name of this index. The name is used to determine what
   * configuration should be applied when thawing indicies from a client
   * configuration.
   * @return The index name.
   */
  public String getName() {
    return getConf().get(NAME_CONF_KEY);
  }

  /**
   * Get the index table for this index. The index is assumed to have complete
   * control over all data encoded in the index table, so that its contents
   * aren't clobbered by other table users.
   * @return The Index table used by this index, or <tt>null</tt> if one has not
   *         be configured for this index
   */
  public TableAdapter getIndexTable() {
    return getTableAdapter(getConf(), INDEX_TABLE_CONF_KEY);
  }

  /**
   * Just get the name of the index table (don't create an adapter).
   * @return The index table used by this index.
   */
  public String getIndexTableName() {
    return getConf().get(INDEX_TABLE_CONF_KEY);
  }

  /**
   * Get the primary table used for this index. This is the table that the index
   * indexes.
   * @return The primary table for this index.
   */
  public TableAdapter getPrimaryTable() {
    return getTableAdapter(getConf(), PRIMARY_TABLE_CONF_KEY);
  }

  /**
   * Just get the name of the primary table for this index, don't create an
   * adapter.
   * @return The primary table name for this index.
   */
  public String getPrimaryTableName() {
    return getConf().get(PRIMARY_TABLE_CONF_KEY);
  }

  /**
   * Gets a table adapter from a configuration.
   * @param conf
   * @param adapterSetting
   * @return a table ready to use, or <tt>null</tt> if none is stored in the
   *         configuration.
   */
  private static TableAdapter getTableAdapter(Configuration conf,
      String adapterSetting) {
    DatabaseAdapter db;
    try {
      db = DatabaseAdapter.readFromConfiguration(conf);
    } catch (RuntimeException e) {
      // short circuit and quit if the db cannot be created
      return null;
    }
    String tableName = conf.get(adapterSetting);
    return db.getTableAdapter(tableName);
  }

  @Override
  public boolean equals(Object o) {
    LexicographicBytesComparator bc = LexicographicBytesComparator.INSTANCE;
    if (o instanceof Index) {
      Index oi = (Index) o;
      return oi.getName().equals(getName())
          && bc.compare(oi.getColumnFamily(), getColumnFamily()) == 0
          && bc.compare(oi.getColumnQualifier(), getColumnQualifier()) == 0
          && oi.getIndexTableName().equals(getIndexTableName())
          && oi.getPrimaryTableName().equals(getPrimaryTableName());
    }
    return false;
  }

  /**
   * Perform any operations necessary to index this put. The passed put is the
   * put for the primary table, not the put to use for the index table.
   * <p>
   * The Index will handle putting into the index table, leaving the
   * {@link Client} to handle assuring the put ends up in the primary table. The
   * index assumes that the column family and column qualifier of values in the
   * {@link Put} already meet the criteria for this index, before being called.
   * 
   * @param put The put to handle.
   */
  public abstract void handlePut(Put put);

  /**
   * Return rowid's between a particular range on the index.
   * @param indexRangeStart The range to start on. An empty array signals to
   *        begin at the beginning of the table.
   * @param indexRangeEnd The range to end on. An empty array signals to end at
   *        the end of the table.
   * @return An iterator of results containing the rowIds of records indexed in
   *         the requested range.
   */
  public abstract SeekingCurrentIterator handleGet(byte[] indexRangeStart,
      byte[] indexRangeEnd);

  /**
   * Primarily for use with MapReduce.
   * @return the range splits associated with this index table as the table is
   *         sharded over the distributed database
   */
  public List<CRange> getSplits() {
    TableAdapter indexTable = getIndexTable();
    byte[][] startkeys = indexTable.getStartKeys();
    byte[][] endKeys = indexTable.getEndKeys();
    List<CRange> range = new ArrayList<CRange>(startkeys.length);
    for (int i = 0; i < startkeys.length; i++) {
      range.add(new CRange(startkeys[i], endKeys[i]));
    }
    return range;
  }

  /**
   * Primarily for use with MapReduce.
   * @return the hosts that this hosting the index table.
   */
  public Collection<String> getPreferredHosts() {
    TableAdapter index = getIndexTable();
    return index.getHosts();
  }
  
  @Override
  public void readFields(DataInput arg0) throws IOException {
    Configuration conf = new Configuration();
    conf.readFields(arg0);
    setConf(conf);
  }

  @Override
  public void write(DataOutput arg0) throws IOException {
    getConf().write(arg0);
  }

  /**
   * Flush all the recent puts to the index table.
   * <p>
   * This should be used with caution as it may cause significant write speed
   * reduction and/or overhead.
   * @throws IOException if the flush fails.
   */
  public void flush() throws IOException {
    TableAdapter table = this.getIndexTable();
    if (table != null)
      table.flush();
  }

  /**
   * Get the {@link Acceptor} associated with this index
   * @return a configured {@link Acceptor}, if one is stored
   * @throws RuntimeException if no valid acceptor is found
   */
  public Acceptor getAcceptor() {
    try {
      // load the acceptor
    Class<? extends Acceptor> clazz = this.getConf().getClass(
        ACCEPTOR_CLASS_KEY, Acceptor.AcceptNone.class, Acceptor.class);
      Acceptor inst = clazz.newInstance();
      // retrieve its configuration
      Configuration conf = ConfUtils.unpackConfigurationInPrefix(
          ACCEPTOR_CONF_KEY, getConf());
      inst.setConf(conf);
      return inst;
    } catch (Exception e) {
      throw new IllegalStateException(
          "Configuration did not store a valid acceptor.", e);
    }
  }


}
