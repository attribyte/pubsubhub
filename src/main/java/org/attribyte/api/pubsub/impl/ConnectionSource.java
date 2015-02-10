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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * A source for database connections.
 */
public interface ConnectionSource {

   /**
    * Initialize the connection source.
    * @param prefix A prefix to be appended to property names.
    * @param props The connection source configuration.
    * @param logger A logger for messages.
    * @throws InitializationException on initialization error.
    */
   public void init(String prefix, Properties props, Logger logger) throws InitializationException;

   /**
    * Gets a database connection.
    * @return The connection.
    * @throws java.sql.SQLException if connection is unavailable.
    */
   public Connection getConnection() throws SQLException;

   /**
    * Gets all metrics associated with the pool.
    * @return The metric set.
    */
   public MetricSet getMetrics();

   /**
    * Shutdown the connection source.
    */
   public void shutdown();
}