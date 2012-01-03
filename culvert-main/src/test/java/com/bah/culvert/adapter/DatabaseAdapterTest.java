package com.bah.culvert.adapter;

import static org.junit.Assert.*;

import java.io.File;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.bah.culvert.mock.MockDatabaseAdapter;
import com.bah.culvert.util.Constants;
import com.google.common.collect.Iterators;

/**
 * Test using the database adapter
 */
public class DatabaseAdapterTest {

  String confFile = "db-test-conf.xml";
  String key = "database.key";
  String value = "someValue";
  /**
   * Test that we pull in the configuration correctly
   */
  @Test
  public void testSourceConfiguration() {
    DatabaseAdapter db = new MockDatabaseAdapter();
    // source it from the string, which should be on the classpath
    db.sourceConfiguration(confFile);
    assertEquals(value, db.getConf().get(key));
    // reset
    db.setConf(null);

    // make sure that path version works equally well
    db.sourceConfiguration(new Path("src" + Path.SEPARATOR + "test"
        + Path.SEPARATOR + "resources" + Path.SEPARATOR + confFile));
    assertEquals("someValue", db.getConf().get("database.key"));

  }

  @Test
  public void testReadFromConfiguration() {
    Configuration conf = new Configuration(false);
    // first check that it returns an empty configuration if not found
    assertEquals(0, Iterators.size(DatabaseAdapter.getDatabaseConfiguration(
        conf).iterator()));

    //source from the file
    conf.set(Constants.DATABASE_CONF_SOURCE, confFile);
    //set that we don't overwrite the value
    conf.set(Constants.DATABASE_CONF_PREFIX+".database.key", "embedded");
    
    Configuration stored = DatabaseAdapter.getDatabaseConfiguration(conf);
    assertEquals(value, stored.get(key));
    
    stored = DatabaseAdapter.getDatabaseConfiguration(conf, false);
    assertEquals("embedded", stored.get(key));
  }
}
