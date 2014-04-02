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

import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.attribyte.api.http.Request;
import org.attribyte.api.pubsub.HubEndpoint;
import org.attribyte.api.pubsub.Callback;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.util.HttpCookieStore;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Creates instances of the default <code>Callback</code> implementation.
 */
public class AsyncCallbackFactory implements org.attribyte.api.pubsub.CallbackFactory {

   @Override
   public Callback create(final Request request, final long subscriptionId, final int priority, final HubEndpoint hub) {
      return new AsyncCallback(request, subscriptionId, priority, hub, callbackTimer, failedCallbackMeter, abandonedCallbackMeter, httpClient);
   }

   @Override
   public Map<String, Metric> getMetrics() {
      return ImmutableMap.<String, Metric>of(
              "callbacks", callbackTimer,
              "failed-callbacks", failedCallbackMeter,
              "abandoned-callbacks", abandonedCallbackMeter
      );
   }

   @Override
   public void init(final Properties props) {

      final int maxConnectionTimeSeconds =
              Integer.parseInt(props.getProperty("asyncCallbackFactory.httpClient.maxConnectionTimeSeconds", "10"));

      final int maxConnectionsPerDeistination =
              Integer.parseInt(props.getProperty("asyncCallbackFactory.httpClient.maxConnectionsPerDestination", "16"));

      final int maxRequestsQueuedPerDestination =
              Integer.parseInt(props.getProperty("asyncCallbackFactory.httpClient.maxRequestsQueuedPerDestination", "1024"));

      final boolean useDispatchIO =
              props.getProperty("asyncCallbackFactory.httpClient.useDispatchIO", "true").equalsIgnoreCase("true");

      final boolean useTCPNoDelay =
              props.getProperty("asyncCallbackFactory.httpClient.useTCPNoDelay", "false").equalsIgnoreCase("true");

      SslContextFactory sslContextFactory = new SslContextFactory();
      this.httpClient = new HttpClient(sslContextFactory);
      this.httpClient.setFollowRedirects(false);
      this.httpClient.setConnectTimeout(maxConnectionTimeSeconds * 1000L);
      this.httpClient.setCookieStore(new HttpCookieStore.Empty());
      this.httpClient.setMaxConnectionsPerDestination(maxConnectionsPerDeistination);
      this.httpClient.setMaxRequestsQueuedPerDestination(maxRequestsQueuedPerDestination);
      this.httpClient.setDispatchIO(useDispatchIO);
      this.httpClient.setTCPNoDelay(useTCPNoDelay);
      try {
         this.httpClient.start();
      } catch(Exception e) { //Signatures need to be changed...
         throw new UnsupportedOperationException("Unable to initialize client", e);
      }
   }

   HttpClient httpClient;

   @Override
   public boolean shutdown(int maxShutdownAwaitSeconds) {
      try {
         this.httpClient.stop();
         return true;
      } catch(Exception e) {
         return false;
      }
   }

   /**
    * Times all callbacks.
    */
   final Timer callbackTimer = new Timer();

   /**
    * Tracks the rate of failed callbacks.
    */
   final Meter failedCallbackMeter = new Meter();

   /**
    * Tracks the rate of abandoned callbacks.
    */
   final Meter abandonedCallbackMeter = new Meter();
}
