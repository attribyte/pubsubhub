/*
 * Copyright 2010 Attribyte, LLC 
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 * 
 * http://www.apache.org/licenses/LICENSE-2.0 
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and limitations under the License.  
 * 
 */

package org.attribyte.api.pubsub.impl;

import com.codahale.metrics.MetricSet;
import com.codahale.metrics.health.HealthCheck;
import org.attribyte.api.InitializationException;
import org.attribyte.api.Logger;
import org.attribyte.api.pubsub.HubDatastore;
import org.attribyte.util.InitUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * A datastore that creates a new connection on every invocation. Not suitable for production environments.
 */
public class SimpleJDBCDatastore extends RDBHubDatastore {
   
   String host;
   String db;
   String user;
   String password;
   String port;
   String driver;
   String connectionString;
   
   @Override
   public void init(final String prefix, final Properties props, final HubDatastore.EventHandler eventHandler, final Logger logger) throws InitializationException {
      
      this.eventHandler = eventHandler;
      this.logger = logger;
      
      InitUtil initProps = new InitUtil(prefix, props);
      
      if(initProps.getProperty("connectionString") == null) {
         this.host = initProps.getProperty("host", null);
         this.port = initProps.getProperty("port", "3306");
         this.db = initProps.getProperty("db", null);
         this.user = initProps.getProperty("user", "");
         this.password = initProps.getProperty("password", "");
         this.driver = initProps.getProperty("driver", "com.mysql.jdbc.Driver");
         try {
            Class.forName("com.mysql.jdbc.Driver");
         } catch(Exception e) {
            throw new InitializationException("Unable to initialize JDBC driver", e);
         }
         
         if(host == null) {
            initProps.throwRequiredException("host");
         }
         
         if(db == null) {
            initProps.throwRequiredException("db");
         }
         
         this.connectionString = "jdbc:mysql://"+host+":"+port+"/"+db;         
      } else {
         this.connectionString = initProps.getProperty("connectionString");
      }
   }

   @Override
   public Connection getConnection() throws SQLException {
      if(user != null && password != null) {
         return DriverManager.getConnection(connectionString, user, password);
      } else {
         return DriverManager.getConnection(connectionString);
      }
   }
   
   @Override
   public void shutdown() {
      //Nothing to do
   }

   @Override
   public Map<String, HealthCheck> getHealthChecks() {
      return Collections.emptyMap();
   }

   @Override
   public MetricSet getMetrics() {
      return null;
   }
}