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
package com.bah.culvert.adapter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;

import com.bah.culvert.data.CColumn;
import com.bah.culvert.util.BaseConfigurable;
import com.bah.culvert.util.ConfUtils;
import com.bah.culvert.util.Constants;

import static com.bah.culvert.util.Constants.*;

/**
 * A connection to a database. Subclasses of this object are used to connect to
 * a database.
 */
public abstract class DatabaseAdapter extends BaseConfigurable implements
    Configurable, Writable {

  public static final byte[][] NO_SPLIT_KEYS = new byte[0][];

  /**
   * Set the configuration from the specified source.
   * <p>
   * Similar to {@link Configuration#addResource(String)}.
   * @param file File name <i>on the classpath</i> from which to source the
   *        configuration
   */
  public void sourceConfiguration(String file) {
    Configuration conf = new Configuration(false);
    conf.addResource(file);
    this.setConf(conf);
  }

  /**
   * Set the configuration from the specified source.
   * <p>
   * Similar to {@link Configuration#addResource(Path)}.
   * @param path Path to the configuration file to use. <b>Ignores the classpath
   *        and just examines the local file system.</b>
   */
  public void sourceConfiguration(Path path){
    Configuration conf = new Configuration(false);
    conf.addResource(path);
    this.setConf(conf);
  }

  /**
   * Get a connection to the specified table
   * 
   * @param tableName to connect to
   * @return a {@link TableAdapter} connecting to the specified database
   */
  public abstract TableAdapter getTableAdapter(String tableName);

  public abstract void create(String tableName, byte[][] splitKeys,
      List<CColumn> columns);

  public void create(String tablename, List<CColumn> columns) {
    create(tablename, NO_SPLIT_KEYS, columns);
  }

  public abstract void delete(String tableName);

  /**
   * Verifies settings to connect to a database and access a table.
   */
  public boolean verify() {
    // noop
    return true;
  }

  public abstract boolean tableExists(String tableName);

  @Override
  public void readFields(DataInput in) throws IOException {
    Configuration conf = new Configuration();
    conf.readFields(in);
    this.verify();

  }

  @Override
  public void write(DataOutput out) throws IOException {
    this.getConf().write(out);
  }

  /**
   * Store the database adapter in a configuration. Can be read back in from
   * {@link #readFromConfiguration(Configuration)}
   * @param db to write
   * @param dbConf database configuration to store
   * @param writeTo {@link Configuration} to write the db intos
   */
  public static void writeToConfiguration(Class<? extends DatabaseAdapter> db,
      Configuration dbConf, Configuration writeTo) {
    // first store the class
    writeTo.setClass(DATABASE_ADAPTER_CLASS_KEY, db, DatabaseAdapter.class);
    // then pack the db adapter conf into the conf
    setDatabaseConfiguration(dbConf, writeTo);
  }

  /**
   * Instantiate and connect to a database adapter from the specified
   * configuration.
   * @param conf to read from. This is expected to a top level, culvert
   *        configuration.
   * @see Constants
   * @return A database adapter ready to be used.
   */
  public static DatabaseAdapter readFromConfiguration(Configuration conf) {
    try {
      // get the database class
      Class<?> dbAdapterClass = conf.getClass(DATABASE_ADAPTER_CLASS_KEY, null);
      Configuration databaseConf = getDatabaseConfiguration(conf);

      // create the adapter
      DatabaseAdapter db = DatabaseAdapter.class.cast(dbAdapterClass
          .newInstance());
      db.setConf(databaseConf);

      // and then make sure it is connected
      db.verify();
      return db;
    } catch (InstantiationException e) {
      throw new RuntimeException(
          "Error instantiating a new DatabaseAdapter object.");
    } catch (IllegalAccessException e) {
      throw new RuntimeException(
          "Error instantiating a new DatabaseAdapter object.");
    }
  }

  /**
   * Get the database configuration packed into the specified conf via
   * {@link #setDatabaseConfiguration(Configuration, Configuration)}.
   * <p>
   * If the database configuration is stored in an external file, it will be
   * used as the primary configuration if
   * {@value Constants#DATABASE_CONF_SOURCE} has been set. This value specifies
   * the configuration file on the classpath from which to source the external
   * configuration. Configuration values packed into the passed in configuration
   * will only be stored if they are not already present in the external
   * configuration.
   * @param conf to read from
   * @return the stored configuration, or an empty one if it cannot be found.
   */
  public static Configuration getDatabaseConfiguration(Configuration conf) {
    return getDatabaseConfiguration(conf, true);
  }

  /**
   * Get the database configuration packed into the specified conf via
   * {@link #setDatabaseConfiguration(Configuration, Configuration)}.
   * <p>
   * If the database configuration is stored in an external file it will be
   * loaded as used as source in the returned configuration, if
   * {@value Constants#DATABASE_CONF_SOURCE} has been set. @return the stored
   * configuration, or an empty one if it cannot be found.
   * <p>
   * The primary configuration is determined by the remaining boolean. For
   * instance, if <tt>true</tt>, the external configuration values will be
   * honored over the embedded values.
   * @param conf to read from
   * @param externalAsPrimary <tt>true</tt> if the external file should have the
   *        overriding values, <tt>false</tt> if the embedded values should take
   *        precedence
   * @return the stored configuration, or an empty one if it cannot be found.
   */
  public static Configuration getDatabaseConfiguration(Configuration conf,
      boolean externalAsPrimary) {
    Configuration imbedded = ConfUtils.unpackConfigurationInPrefix(
        DATABASE_CONF_PREFIX, conf);
    String externalSource = conf.get(DATABASE_CONF_SOURCE);
    if (externalSource == null)
      return imbedded;

    // get the external configuration
    Configuration external = new Configuration(false);
    external.addResource(externalSource);

    // return the updated config
    return externalAsPrimary ? ConfUtils.combineConfigurations(external,
        imbedded) : ConfUtils.combineConfigurations(imbedded, external);
  }

  /**
   * Set the configuration to use with the database. It can be read back out via
   * {@link #getDatabaseConfiguration(Configuration)}
   * @param databaseConf to use
   * @param conf to set in
   */
  public static void setDatabaseConfiguration(Configuration databaseConf,
      Configuration conf) {
    ConfUtils.packConfigurationInPrefix(DATABASE_CONF_PREFIX, databaseConf,
        conf);
  }
}
