package org.attribyte.api.pubsub.impl;

import org.attribyte.api.ConsoleLogger;
import org.attribyte.api.InitializationException;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Tests for the MySQL implementation.
 * <p>
 *    Note that the tests should start with an empty pubsub_test
 *    database.
 * </p>
 */
public class MySQLDatastoreTest extends HubDatastoreTest<MySQLDatastore> {

   private static AtomicBoolean isInit = new AtomicBoolean(false);
   private static MySQLDatastore _datastore;

   protected MySQLDatastore createDatastore() throws InitializationException {
      if(isInit.compareAndSet(false, true)) {
         MySQLDatastore datastore = new MySQLDatastore();
         Properties props = new Properties();
         props.setProperty("user", "root");
         props.setProperty("driver", "com.mysql.jdbc.Driver");
         props.setProperty("connectionString", "jdbc:mysql://127.0.0.1/pubsub_test");
         props.setProperty("endpoint.connectionsClass", "org.attribyte.api.pubsub.impl.SimpleConnectionSource");
         datastore.init("", props, null, new ConsoleLogger());
         _datastore = datastore;
      }
      return _datastore;
   }
}