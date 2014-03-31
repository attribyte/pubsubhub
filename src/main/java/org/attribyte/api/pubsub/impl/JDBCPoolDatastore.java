/*
 * Copyright 2010, 2014 Attribyte, LLC
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
import com.google.common.collect.ImmutableMap;
import org.attribyte.api.InitializationException;
import org.attribyte.api.Logger;
import org.attribyte.api.pubsub.HubDatastore;
import org.attribyte.sql.pool.ConnectionPool;
import org.attribyte.util.InitUtil;
import org.attribyte.util.SQLUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A datastore that uses a connection pool (acp) to create database connections.
 * <p>
 * The test SQL for connection health check is 'SELECT 1' by default.
 * This may be configured by setting the property 'db.testSQL'.
 * </p>
 */
public class JDBCPoolDatastore extends RDBHubDatastore {

   @Override
   public void init(String prefix, Properties props, HubDatastore.EventHandler eventHandler, Logger logger) throws InitializationException {
      if(isInit.compareAndSet(false, true)) {
         this.eventHandler = eventHandler;
         this.logger = logger;
         this.connectionTestSQL = props.getProperty(prefix + "db.testSQL", "SELECT 1");
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

   /**
    * Gets all metrics associated with the pool.
    * @return The metric set.
    */
   public MetricSet getMetrics() {
      return pool.getMetrics();
   }

   @Override
   public Map<String, HealthCheck> getHealthChecks() {
      return ImmutableMap.<String, HealthCheck>of(
              "datastore-connection",
              new HealthCheck() {
                 @Override
                 protected Result check() throws Exception {
                    Connection conn = null;
                    Statement stmt = null;
                    ResultSet rs = null;
                    try {
                       conn = getConnection();
                       stmt = conn.createStatement();
                       rs = stmt.executeQuery(connectionTestSQL);
                       if(rs.next()) {
                          return HealthCheck.Result.healthy();
                       } else {
                          return HealthCheck.Result.unhealthy(connectionTestSQL + " failed");
                       }
                    } catch(Exception e) {
                       return HealthCheck.Result.unhealthy("Database connection problem", e);
                    } finally {
                       SQLUtil.closeQuietly(conn, stmt, rs);
                    }
                 }
              }
      );
   }

   private ConnectionPool pool;
   private String connectionTestSQL = "SELECT 1";
   private final AtomicBoolean isInit = new AtomicBoolean(false);
}