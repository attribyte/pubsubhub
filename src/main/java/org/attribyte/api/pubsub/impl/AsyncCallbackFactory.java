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
      return new AsyncCallback(request, subscriptionId, priority, hub, callbackTimer, failedCallbackMeter, abandonedCallbackMeter, callbackExecutor);
   }

   @Override
   public Map<String, Metric> getMetrics() {
      return ImmutableMap.<String, Metric>of(
              "callbacks", callbackTimer,
              "failed-callbacks", failedCallbackMeter,
              "abandoned-callbacks", abandonedCallbackMeter,
              "enqueued-callbacks", callbackQueueSize
      );
   }

   @Override
   public void init(final Properties props) {
      int maxQueueSize = Integer.parseInt(props.getProperty("asyncCallbackFactory.maxQueueSize", "4096"));
      int minPoolSize = Integer.parseInt(props.getProperty("asyncCallbackFactory.minPoolSize", "1"));
      int maxPoolSize = Integer.parseInt(props.getProperty("asyncCallbackFactory.maxPoolSize", "64"));
      final int gaugeCacheTimeSeconds =
              Integer.parseInt(props.getProperty("asyncCallbackFactory.gaugeCacheTimeSeconds", "15"));

      if(maxQueueSize > 0) {
         callbackQueue = new ArrayBlockingQueue<Runnable>(maxQueueSize);
      } else {
         callbackQueue = new LinkedBlockingQueue<Runnable>();
      }

      this.callbackQueueSize = new CachedGauge<Integer>(gaugeCacheTimeSeconds, TimeUnit.SECONDS) {
         @Override
         protected Integer loadValue() {
            return callbackQueue.size();
         }
      };

      ThreadPoolExecutor executor = new ThreadPoolExecutor(minPoolSize, maxPoolSize, 0L, TimeUnit.MILLISECONDS,
              callbackQueue, new ThreadFactoryBuilder().setNameFormat("async-publisher-%d").build());
      executor.prestartAllCoreThreads();
      this.callbackExecutor = MoreExecutors.listeningDecorator(executor);
   }

   @Override
   public boolean shutdown(int maxShutdownAwaitSeconds) {
      callbackExecutor.shutdown();
      try {
         boolean terminatedNormally = callbackExecutor.awaitTermination(maxShutdownAwaitSeconds, TimeUnit.SECONDS);
         if(!terminatedNormally) {
            callbackExecutor.shutdownNow();
         }
         return terminatedNormally;
      } catch(InterruptedException ie) {
         Thread.currentThread().interrupt();
         return false;
      }
   }

   /**
    * callbackExecutor.awaitTermination()
    *
    * The queue that holds callbacks waiting to execute.
    */
   BlockingQueue<Runnable> callbackQueue;

   /**
    * The executor service for async callback.
    */
   ListeningExecutorService callbackExecutor;

   /**
    * A gauge that measures the callback queue size.
    */
   CachedGauge<Integer> callbackQueueSize;

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
