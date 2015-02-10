/*
 * Copyright 2015 Attribyte, LLC
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
import org.attribyte.api.InitializationException;
import org.attribyte.api.Logger;
import org.attribyte.sql.pool.ConnectionPool;
import org.attribyte.util.InitUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A datastore that uses a connection pool (acp) to create database connections.
 */
public class PoolConnectionSource implements ConnectionSource {

   @Override
   public void init(String prefix, Properties props, Logger logger) throws InitializationException {
      if(isInit.compareAndSet(false, true)) {
         Properties poolProps = new InitUtil(prefix + "acp.", props, false).getProperties();
         List<ConnectionPool.Initializer> pools = ConnectionPool.Initializer.fromProperties(poolProps, null, logger);
         if(pools.size() == 0) {
            throw new InitializationException("No connection pool specified");
         } else {
            try {
               this.pool = pools.get(0).createPool();
            } catch(SQLException se) {
               throw new InitializationException("Unable to create JDBC pool", se);
            }
         }
      }
   }

   @Override
   public final Connection getConnection() throws SQLException {
      return pool.getConnection();
   }

   @Override
   public final void shutdown() {
      pool.shutdown();
   }

   @Override
   public MetricSet getMetrics() {
      return pool.getMetrics();
   }

   private ConnectionPool pool;
   private final AtomicBoolean isInit = new AtomicBoolean(false);
}