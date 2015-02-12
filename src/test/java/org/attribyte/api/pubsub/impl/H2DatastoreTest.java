package org.attribyte.api.pubsub.impl;

import org.attribyte.api.ConsoleLogger;
import org.attribyte.api.InitializationException;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class H2DatastoreTest extends HubDatastoreTest<H2Datastore> {

   private static AtomicBoolean isInit = new AtomicBoolean(false);
   private static H2Datastore _datastore;

   protected H2Datastore createDatastore() throws InitializationException {
      if(isInit.compareAndSet(false, true)) {
         H2Datastore datastore = new H2Datastore();
         Properties props = new Properties();
         props.setProperty("driver", "org.h2.Driver");
         props.setProperty("endpoint.connectionsClass", "org.attribyte.api.pubsub.impl.SimpleConnectionSource");
         props.setProperty("connectionString", "jdbc:h2:mem:pubsub;IGNORECASE=TRUE;DB_CLOSE_DELAY=-1");
         props.setProperty("h2.initFile", "/home/matt/devel/attribyte/git/pubsubhub/config/h2_pubsub_hub.sql"); //TODO
         datastore.init("", props, null, new ConsoleLogger());
         _datastore = datastore;
      }
      return _datastore;
   }
}