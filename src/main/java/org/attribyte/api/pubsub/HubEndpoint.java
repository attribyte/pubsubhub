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

package org.attribyte.api.pubsub;

import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.attribyte.api.DatastoreException;
import org.attribyte.api.InitializationException;
import org.attribyte.api.InvalidURIException;
import org.attribyte.api.Logger;
import org.attribyte.api.http.AuthScheme;
import org.attribyte.api.http.Client;
import org.attribyte.api.http.Request;
import org.attribyte.api.http.Response;
import org.attribyte.api.http.ResponseBuilder;
import org.attribyte.util.InitUtil;
import org.attribyte.util.StringUtil;
import org.attribyte.util.URIEncoder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A pubsubhubub hub.
 * @author Attribyte, LLC
 */
public class HubEndpoint implements MetricSet {

   /**
    * Creates an uninitialized endpoint. Required for reflected instantiation.
    * <p>
    * The <code>init</code> method must be called to initialize the endpoint.
    * </p>
    * @see #init(String, Properties, Logger, org.attribyte.api.pubsub.HubDatastore.EventHandler)
    */
   public HubEndpoint() {
   }

   /**
    * Creates an initialized endpoint.
    * @param prefix The property prefix.
    * @param props The properties.
    * @param logger The logger.
    * @param eventHandler The (optional) event handler.
    * @throws InitializationException on initialization error.
    */
   public HubEndpoint(String prefix, Properties props, Logger logger,
                      HubDatastore.EventHandler eventHandler) throws InitializationException {
      init(prefix, props, logger, eventHandler);
   }

   /**
    * Creates an initialized endpoint with specified topic and callback filters.
    * @param prefix The property prefix.
    * @param props The properties.
    * @param logger The logger.
    * @param eventHandler The (optional) event handler.
    * @param topicURLFilters A list of topic URL filters to add after any initialized filters.
    * @param callbackURLFilters A list of callback URL filters to add after any initialized filters.
    * @throws InitializationException on initialization error.
    */
   public HubEndpoint(String prefix, Properties props, Logger logger, HubDatastore.EventHandler eventHandler,
                      List<URLFilter> topicURLFilters, List<URLFilter> callbackURLFilters) throws InitializationException {
      init(prefix, props, logger, eventHandler);

      if(topicURLFilters != null && topicURLFilters.size() > 0) {
         if(this.topicURLFilters == null || this.topicURLFilters.size() == 0) {
            this.topicURLFilters = topicURLFilters;
         } else {
            this.topicURLFilters.addAll(topicURLFilters);
         }
      }

      if(callbackURLFilters != null && callbackURLFilters.size() > 0) {
         if(this.callbackURLFilters == null || this.callbackURLFilters.size() == 0) {
            this.callbackURLFilters = callbackURLFilters;
         } else {
            this.callbackURLFilters.addAll(callbackURLFilters);
         }
      }
   }

   /**
    * Gets the maximum accepted HTTP parameter size in bytes.
    * @return The maximum size.
    */
   public int getMaxParameterBytes() {
      return maxParameterBytes;
   }

   /**
    * Gets the default encoding.
    * @return The encoding.
    */
   public String getDefaultEncoding() {
      return defaultEncoding;
   }

   /**
    * Gets the datastore.
    * @return The datastore.
    */
   public HubDatastore getDatastore() {
      return datastore;
   }

   /**
    * Gets the logger.
    * @return The logger.
    */
   public Logger getLogger() {
      return logger;
   }

   /**
    * Gets the hub HTTP client.
    * @return The HTTP client.
    */
   public Client getHttpClient() {
      return httpClient;
   }

   /**
    * Gets the user agent sent with HTTP requests.
    * @return The user agent.
    */
   public String getUserAgent() {
      return userAgent;
   }

   /**
    * The minimum allowed subscription lease.
    * @return The minimum lease.
    */
   public int getMinLeaseSeconds() {
      return minLeaseSeconds;
   }

   /**
    * The maximum allowed subscription lease.
    * @return The maximum lease.
    */
   public int getMaxLeaseSeconds() {
      return maxLeaseSeconds;
   }

   /**
    * Initialize the hub from properties.
    * <p>
    * The following properties are available. <b>Bold</b> properties are required.
    * <h2>General</h2>
    * <dl>
    * <dt>maxParameterBytes</dt>
    * <dd>The maximum number of bytes allowed in any parameter. Default is 1024.</dd>
    * <dt>maxShutdownAwaitSeconds</dt>
    * <dd>The maximum number of seconds to await for all notifications, callbacks, etc. to complete on
    * shutdown request. Default 30s.</dd>
    * </dl>
    *
    * <h2>Datastore</h2>
    * <dl>
    * <dt><b>datastoreClass</b></dt>
    * <dd>A class that implements <code>Datastore</code> to provide read/write access to persistent data.</dd>
    * </dl>
    *
    * <h2>HTTP Client</h2>
    * <dl>
    * <dt>httpclient.class<dt>
    * <dd>The HTTP client implementation. If unspecified, default is <code>org.attribyte.api.http.impl.commons.Client</code>.</dd>
    * <dt><b>httpclient.userAgent</b></dt>
    * <dd>The User-Agent string sent with all requests.</dd>
    * <dt><b>httpclient.connectionTimeoutMillis</b></dt>
    * <dd>The HTTP connection timeout in milliseconds.</dd>
    * <dt><b>httpclient.socketTimeoutMillis</b></dt>
    * <dd>The HTTP client socket timeout in milliseconds.</dd>
    * <dt>httpclient.proxyHost</dt>
    * <dd>The HTTP proxy host. If specified, all client requests will use this proxy.</dd>
    * <dt>httpclient.proxyPort</dt>
    * <dd>The HTTP proxy port. Required when <code>proxyHost</code> is specified</dd>
    * </dl>
    *
    * <h2>Notifications</h2>
    *
    * <h3>Notifiers</h3>
    * <dl>
    * <dt><b>notifierFactoryClass</b></dt>
    * <dd>Implementation of <code>NotifierFactory</code>. Creates instances of (<code>Runnable</code>) <code>Notifier</code>
    * used to schedule send of <code>Notification</code> to all subscribers.</dd>
    * <dt><b>maxConcurrentNotifiers</b></dt>
    * <dd>The maximum number of concurrent notifiers.</dd>
    * <dt>baseConcurrentNotifiers</dt>
    * <dd>The minimum number of threads waiting to execute notifiers.</dd>
    * <dt>maxNotifierQueueSize</dt>
    * <dd>The maximum number of notifiers queued when all threads are busy.</dd>
    * <dt>notifierThreadKeepAliveMinutes</dt>
    * <dd>The number of minutes notifier threads remain idle.</dd>
    * <dt>notifierExecutorServiceClass</dt>
    * <dd>A user-defined service for executing notifiers.
    * Must implement <code>ExecutorService</code> and have a default initializer.
    * </dd>
    * </dl>
    *
    * <h3>Subscriber Callback</h3>
    * <dl>
    * <dt><b>maxConcurrentCallbacks</b></dt>
    * <dd>The maximum number of concurrent callbacks.</dd>
    * <dt>callbackThreadKeepAliveMinutes</dt>
    * <dd>The number of minutes callback threads remain idle.</dd>
    * <dt>callbackExecutorServiceClass</dt>
    * <dd>A user-defined service for executing callback.
    * Must implement <code>ExecutorService</code> and have a default initializer.
    * </dd>
    * <dt>maxConcurrentFailedCallbacks</dt>
    * <dd>The maximum number of failed callbacks concurrently retried.</dd>
    * <dt>failedCallbackRetryStrategyClass</dt>
    * <dd>The failed callback retry strategy. Must implement <code>RetryStrategy</code>. Default is exponential backoff.</dd>
    * <dt>failedCallbackRetryMaxAttempts</dt>
    * <dd>The maximum number of failed callback retry attempts. Default is <code>14</code>.</dd>
    * <dt>failedCallbackRetryDelayIntervalMillis</dt>
    * <dd>The callback retry delay interval. Default is <code>100</code> milliseconds.</dd>
    * </dl>
    * <h2>Subscriptions</h2>
    * <dl>
    * <dt><b>verifierFactoryClass</b></dt>
    * <dd>Implementation of <code>VerifierFactory</code>. Creates instances of (<code>Runnable</code>) <code>Verifier</code>
    * </dd>
    * <dt><b>maxConcurrentVerifiers</b></dt>
    * <dd>The maximum number of concurrent subscription verifiers.</dd>
    * <dt>baseConcurrentVerifiers</dt>
    * <dd>The minimum number of threads waiting to verify subscriptions.</dd>
    * <dt>maxVerifierQueueSize</dt>
    * <dd>The maximum number of subscription verifications queued when all callback threads are busy.</dd>
    * <dt>verifierThreadKeepAliveMinutes</dt>
    * <dd>The number of minutes subscription verifier threads remain idle.</dd>
    * <dt>verifierExecutorServiceClass</dt>
    * <dd>A user-defined executor service for subscription verification.
    * Must implement <code>ExecutorService</code> and have a default initializer.
    * </dd>
    * <dt>verifyRetryWaitMinutes</dt>
    * <dd>The minimum number of minutes before (async) verify retry if initial verify fails.
    * Default is 10 minutes.
    * </dd>
    * <dt>verifyRetryLimit</dd>
    * <dd>The maximum number of verify retries. Default 10.</dd>
    * <dt><b>verifyRetryThreads</b></dt>
    * <dd>The number of threads available to handle verify retry.</dd>
    * <dt>topicURLFilters</dt>
    * <dd>A space (or comma) separated list of fully-qualified <code>URLFilters</code> to be applied to the topic
    * URL of any subscriptions. Filters are applied, in the order they appear, before any subscription processing.</dd>
    * <dt>callbackURLFilters</dt>
    * <dd>A space (or comma) separated list of fully-qualified <code>URLFilters</code> to be applied to the callback
    * URL of any subscriptions. Filters are applied, in the order they appear, before any subscription processing.</dd>
    * <dt><b>minLeaseSeconds</b></dt>
    * <dd>The minimum allowed lease time.</dd>
    * <dt><b>maxLeaseSeconds</b></dt>
    * <dd>The maximum allowed lease time.</dd>
    * </dl>
    * </p>
    * @param prefix The prefix for all properties (e.g. 'hub.').
    * @param props The properties.
    * @param logger The logger. If unspecified, messages are logged to the console.
    * @param eventHandler A datastore event handler.
    * @throws InitializationException on initialization error.
    */
   public void init(final String prefix, final Properties props, final Logger logger,
                    final HubDatastore.EventHandler eventHandler) throws InitializationException {

      if(isInit.compareAndSet(false, true)) {

         InitUtil initUtil = new InitUtil(prefix, props);

         datastore = (HubDatastore)initUtil.initClass("datastoreClass", HubDatastore.class);
         if(datastore == null) {
            initUtil.throwRequiredException("datastoreClass");
         }

         this.logger = logger;

         datastore.init(prefix, props, eventHandler, logger);

         userAgent = initUtil.getProperty("httpclient.userAgent");
         if(!StringUtil.hasContent(userAgent)) {
            initUtil.throwRequiredException("httpclient.userAgent");
         }

         httpClient = (Client)initUtil.initClass("httpclient.class", Client.class);
         if(httpClient == null) {
            initUtil.throwRequiredException("httpclient.class");
         }
         httpClient.init(prefix + "httpclient.", props, logger);

         notifierFactory = (NotifierFactory)initUtil.initClass("notifierFactoryClass", NotifierFactory.class);
         if(notifierFactory == null) {
            initUtil.throwRequiredException("notifierFactoryClass");
         } else {
            notifierFactory.init(new InitUtil(prefix, props, false).getProperties()); //Don't lower-case!
         }

         String notifierExecutorServiceClass = initUtil.getProperty("notifierExecutorServiceClass");
         if(notifierExecutorServiceClass != null) {
            notifierService = (ExecutorService)initUtil.initClass("notifierExecutorServiceClass", ExecutorService.class);
            notifierServiceQueueSize = null;
         } else {
            int baseConcurrentNotifiers = initUtil.getIntProperty("baseConcurrentNotifiers", 0);
            int maxNotifierQueueSize = initUtil.getIntProperty("maxNotifierQueueSize", 0);
            int maxConcurrentNotifiers = initUtil.getIntProperty("maxConcurrentNotifiers", 0);
            int notifierThreadKeepAliveMinutes = initUtil.getIntProperty("notifierThreadKeepAliveMinutes", 1);

            if(maxConcurrentNotifiers < 1) {
               initUtil.throwPositiveIntRequiredException("maxConcurrentNotifiers");
            }

            if(maxNotifierQueueSize > 0) {

               final BlockingQueue<Runnable> queue = new ArrayBlockingQueue<Runnable>(maxNotifierQueueSize, true); //Fair
               notifierServiceQueueSize = new CachedGauge<Integer>(15, TimeUnit.SECONDS) {
                  @Override
                  protected Integer loadValue() {
                     return queue.size();
                  }
               };

               notifierService = new ThreadPoolExecutor(baseConcurrentNotifiers > 0 ? baseConcurrentNotifiers : 1, maxConcurrentNotifiers,
                       notifierThreadKeepAliveMinutes, TimeUnit.MINUTES, queue,
                       new ThreadFactoryBuilder().setNameFormat("notifier-executor-%d").build());
            } else {

               final BlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>();
               notifierServiceQueueSize = new CachedGauge<Integer>(15, TimeUnit.SECONDS) {
                  @Override
                  protected Integer loadValue() {
                     return queue.size();
                  }
               };

               notifierService = new ThreadPoolExecutor(maxConcurrentNotifiers, maxConcurrentNotifiers,
                       notifierThreadKeepAliveMinutes, TimeUnit.MINUTES, queue,
                       new ThreadFactoryBuilder().setNameFormat("notifier-executor-%d").build());
            }
         }

         String callbackExecutorServiceClass = initUtil.getProperty("callbackExecutorServiceClass");
         if(callbackExecutorServiceClass != null) {
            callbackService = (ExecutorService)initUtil.initClass("callbackExecutorServiceClass", ExecutorService.class);
            callbackServiceQueueSize = null;
         } else {
            int maxConcurrentCallbacks = initUtil.getIntProperty("maxConcurrentCallbacks", 0);
            int callbackThreadKeepAliveMinutes = initUtil.getIntProperty("callbackThreadKeepAliveMinutes", 1);

            if(maxConcurrentCallbacks < 1) {
               initUtil.throwPositiveIntRequiredException("maxConcurrentCallbacks");
            }

            final BlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>();
            callbackServiceQueueSize = new CachedGauge<Integer>(15, TimeUnit.SECONDS) {
               @Override
               protected Integer loadValue() {
                  return queue.size();
               }
            };

            callbackService = new ThreadPoolExecutor(maxConcurrentCallbacks, maxConcurrentCallbacks,
                    callbackThreadKeepAliveMinutes, TimeUnit.MINUTES, queue,
                    new ThreadFactoryBuilder().setNameFormat("callback-executor-%d").build());
         }

         int maxConcurrentFailedCallbacks = initUtil.getIntProperty("maxConcurrentFailedCallbacks", 4);
         failedCallbackService = Executors.newScheduledThreadPool(maxConcurrentFailedCallbacks,
                 new ThreadFactoryBuilder().setNameFormat("failed-callback-executor-%d").build());

         String failedCallbackRetryStrategyClass = initUtil.getProperty("failedCallbackRetryStrategyClass");
         if(failedCallbackRetryStrategyClass != null) {
            failedCallbackRetryStrategy = (RetryStrategy)initUtil.initClass("failedCallbackRetryStrategyClass", RetryStrategy.class);
            failedCallbackRetryStrategy.init(initUtil.getProperties());
         } else {
            int maxAttempts = initUtil.getIntProperty("failedCallbackRetryMaxAttempts", 14);
            long delayIntervalMillis = initUtil.getIntProperty("failedCallbackRetryDelayIntervalMillis", 100);
            failedCallbackRetryStrategy = new RetryStrategy.ExponentialBackoff(maxAttempts, delayIntervalMillis);
         }

         String disableSubscriptionStrategyClass = initUtil.getProperty("disableSubscriptionStrategyClass");
         if(disableSubscriptionStrategyClass != null) {
            disableSubscriptionStrategy = (DisableSubscriptionStrategy)initUtil.initClass("disableSubscriptionStrategyClass", DisableSubscriptionStrategy.class);
            disableSubscriptionStrategy.init(initUtil.getProperties());
         } else {
            disableSubscriptionStrategy = DisableSubscriptionStrategy.NEVER_DISABLE;
         }

         verifierFactory = (SubscriptionVerifierFactory)initUtil.initClass("verifierFactoryClass", SubscriptionVerifierFactory.class);
         if(verifierFactory == null) {
            initUtil.throwRequiredException("verifierFactoryClass");
         } else {
            verifierFactory.init(initUtil.getProperties());
         }

         String verifierExecutorServiceClass = initUtil.getProperty("verifierExecutorServiceClass");
         if(verifierExecutorServiceClass != null) {
            verifierService = (ExecutorService)initUtil.initClass("verifierExecutorServiceClass", ExecutorService.class);
         } else {
            int baseConcurrentVerifiers = initUtil.getIntProperty("baseConcurrentVerifiers", 0);
            int maxVerifierQueueSize = initUtil.getIntProperty("maxVerifierQueueSize", 0);
            int maxConcurrentVerifiers = initUtil.getIntProperty("maxConcurrentVerifiers", 0);
            int verifierThreadKeepAliveMinutes = initUtil.getIntProperty("verifierThreadKeepAliveMinutes", 1);

            if(maxConcurrentVerifiers < 1) {
               initUtil.throwPositiveIntRequiredException("maxConcurrentVerifiers");
            }

            if(maxVerifierQueueSize > 0) {
               verifierService = new ThreadPoolExecutor(baseConcurrentVerifiers > 0 ? baseConcurrentVerifiers : 1, maxConcurrentVerifiers,
                       verifierThreadKeepAliveMinutes, TimeUnit.MINUTES, new ArrayBlockingQueue<Runnable>(maxVerifierQueueSize, true),
                       new ThreadFactoryBuilder().setNameFormat("verifier-executor-%d").build());
            } else {
               verifierService = new ThreadPoolExecutor(maxConcurrentVerifiers, maxConcurrentVerifiers,
                       verifierThreadKeepAliveMinutes, TimeUnit.MINUTES, new LinkedBlockingQueue<Runnable>(),
                       new ThreadFactoryBuilder().setNameFormat("verifier-executor-%d").build());
            }
         }

         minLeaseSeconds = initUtil.getIntProperty("minLeaseSeconds", 3600);

         if(initUtil.getProperty("maxLeaseDays") != null) {
            maxLeaseSeconds = initUtil.getIntProperty("maxLeaseDays", 1) * 3600 * 24;
         } else {
            maxLeaseSeconds = initUtil.getIntProperty("maxLeaseSeconds", 3600 * 24);
         }

         maxShutdownAwaitSeconds = initUtil.getIntProperty("maxShutdownAwaitSeconds", 30);

         verifyRetryWaitMinutes = initUtil.getIntProperty("verifyRetryWaitMinutes", 10);
         verifyRetryLimit = initUtil.getIntProperty("verifyRetryLimit", 10);

         int verifyRetryThreads = initUtil.getIntProperty("verifyRetryThreads", 0);
         if(verifyRetryThreads < 1) {
            initUtil.throwPositiveIntRequiredException("verifyRetryThreads");
         }
         verifierRetryService = new ScheduledThreadPoolExecutor(verifyRetryThreads,
                 new ThreadFactoryBuilder().setNameFormat("verifier-retry-executor-%d").build());

         expirationService.scheduleWithFixedDelay(
                 new Runnable() {
                    @Override
                    public void run() {
                       try {
                          datastore.expireSubscriptions(1000);
                       } catch(Throwable t) {
                          logger.error("Problem expiring subscriptions", t);
                       }
                    }
                 }, 0, 15, TimeUnit.MINUTES
         );

         List<Object> topicURLFilterObjects = initUtil.initClassList("topicURLFilters", URLFilter.class);
         if(topicURLFilterObjects.size() > 0) {
            topicURLFilters = new ArrayList<URLFilter>(topicURLFilterObjects.size() + 1);
            topicURLFilters.add(new FragmentRejectFilter());
            for(Object o : topicURLFilterObjects) {
               topicURLFilters.add((URLFilter)o);
            }
            topicURLFilters = Collections.unmodifiableList(topicURLFilters);
         } else {
            topicURLFilters = new ArrayList<URLFilter>(1);
            topicURLFilters.add(new FragmentRejectFilter());
            topicURLFilters = Collections.unmodifiableList(topicURLFilters);
         }

         for(URLFilter filter : topicURLFilters) {
            filter.init(initUtil.getProperties());
         }

         List<Object> callbackURLFilterObjects = initUtil.initClassList("callbackURLFilters", URLFilter.class);
         if(callbackURLFilterObjects.size() > 0) {
            callbackURLFilters = new ArrayList<URLFilter>(callbackURLFilterObjects.size() + 1);
            callbackURLFilters.add(new FragmentRejectFilter());
            for(Object o : callbackURLFilterObjects) {
               callbackURLFilters.add((URLFilter)o);
            }
            callbackURLFilters = Collections.unmodifiableList(callbackURLFilters);
         } else {
            callbackURLFilters = new ArrayList<URLFilter>(1);
            callbackURLFilters.add(new FragmentRejectFilter());
            callbackURLFilters = Collections.unmodifiableList(callbackURLFilters);
         }

         for(URLFilter filter : callbackURLFilters) {
            filter.init(initUtil.getProperties());
         }
      }
   }

   /**
    * Shutdown the hub, releasing all resources.
    */
   public void shutdown() {
      if(isShutdown.compareAndSet(false, true)) {
         logger.info("Endpoint shutdown started...");

         logger.info("Shutting down expiration service...");
         expirationService.shutdownNow();
         logger.info("Expiration service shutdown normally.");

         try {

            logger.info("Shutting down filters...");

            for(URLFilter filter : topicURLFilters) {
               filter.shutdown(maxShutdownAwaitSeconds);
            }

            for(URLFilter filter : callbackURLFilters) {
               filter.shutdown(maxShutdownAwaitSeconds);
            }

            logger.info("Shutting down notifier service...");
            long startMillis = System.currentTimeMillis();
            notifierService.shutdown();
            boolean terminatedNormally = notifierService.awaitTermination(maxShutdownAwaitSeconds, TimeUnit.SECONDS);
            long elapsedMillis = System.currentTimeMillis() - startMillis;
            if(terminatedNormally) {
               logger.info("Notifier service shutdown normally in " + elapsedMillis + " ms.");
            } else {
               notifierService.shutdownNow();
               logger.info("Notifier service shutdown *abnormally* in " + elapsedMillis + " ms.");
            }

            logger.info("Shutting down callback service...");
            startMillis = System.currentTimeMillis();
            callbackService.shutdown();
            terminatedNormally = callbackService.awaitTermination(maxShutdownAwaitSeconds, TimeUnit.SECONDS);
            elapsedMillis = System.currentTimeMillis() - startMillis;
            if(terminatedNormally) {
               logger.info("Callback service shutdown normally in " + elapsedMillis + " ms.");
            } else {
               callbackService.shutdownNow();
               logger.info("Callback service shutdown *abnormally* in " + elapsedMillis + " ms.");
            }

            logger.info("Shutting down verifier service...");
            startMillis = System.currentTimeMillis();
            verifierService.shutdown();
            terminatedNormally = verifierService.awaitTermination(maxShutdownAwaitSeconds, TimeUnit.SECONDS);
            elapsedMillis = System.currentTimeMillis() - startMillis;
            if(terminatedNormally) {
               logger.info("Verifier service shutdown normally in " + elapsedMillis + " ms.");
            } else {
               verifierService.shutdownNow();
               logger.info("Verifier service shutdown *abnormally* in " + elapsedMillis + " ms.");
            }

            logger.info("Shutting down verifier retry service...");
            startMillis = System.currentTimeMillis();
            verifierRetryService.shutdown();
            terminatedNormally = verifierRetryService.awaitTermination(maxShutdownAwaitSeconds, TimeUnit.SECONDS);
            elapsedMillis = System.currentTimeMillis() - startMillis;
            if(terminatedNormally) {
               logger.info("Verifier retry service shutdown normally in " + elapsedMillis + " ms.");
            } else {
               verifierRetryService.shutdownNow();
               logger.info("Verifier retry service shutdown *abnormally* in " + elapsedMillis + " ms.");
            }

            logger.info("Shutting down failed callback service...");
            startMillis = System.currentTimeMillis();
            failedCallbackService.shutdown();
            terminatedNormally = failedCallbackService.awaitTermination(maxShutdownAwaitSeconds, TimeUnit.SECONDS);
            elapsedMillis = System.currentTimeMillis() - startMillis;
            if(terminatedNormally) {
               logger.info("Failed callback service shutdown normally in " + elapsedMillis + " ms.");
            } else {
               failedCallbackService.shutdownNow();
               logger.info("Failed callback service shutdown *abnormally* in " + elapsedMillis + " ms.");
            }

            logger.info("Shutting down notifier factory...");
            startMillis = System.currentTimeMillis();
            terminatedNormally = notifierFactory.shutdown(maxShutdownAwaitSeconds);
            elapsedMillis = System.currentTimeMillis() - startMillis;
            if(terminatedNormally) {
               logger.info("Notifier factory shutdown normally in " + elapsedMillis + " ms.");
            } else {
               logger.info("Notifier factory shutdown *abnormally* in " + elapsedMillis + " ms.");
            }

            logger.info("Shutting down verifier factory...");
            startMillis = System.currentTimeMillis();
            terminatedNormally = verifierFactory.shutdown(maxShutdownAwaitSeconds);
            elapsedMillis = System.currentTimeMillis() - startMillis;
            if(terminatedNormally) {
               logger.info("Verifier factory shutdown normally in " + elapsedMillis + " ms.");
            } else {
               logger.info("Verifier factory shutdown *abnormally* in " + elapsedMillis + " ms.");
            }

         } catch(InterruptedException ie) {
            Thread.currentThread().interrupt();
         }

         try {
            httpClient.shutdown();
         } catch(Exception e) {
            logger.error("HTTP client shutdown abnormally", e);
         }
         datastore.shutdown();

         logger.info("Endpoint shutdown complete.");
      }
   }

   /**
    * Enqueue a new content notification.
    * <p>
    * A notification identifies new content for a topic. When
    * the notification is processed, all topic subscribers are notified
    * at their callback URL.
    * </p>
    * @param notification The notification.
    */
   public void enqueueNotification(final Notification notification) {
      notifierService.execute(notifierFactory.create(notification, this));
   }

   /**
    * Handles client subscription requests.
    * @param request The HTTP request.
    * @return The HTTP response.
    */
   public Response subscriptionRequest(Request request) {

      if(topicURLFilters != null) {
         String topicURL;
         try {
            topicURL = urlDecoder.recode(request.getParameterValue("hub.topic"));
         } catch(Exception e) {
            return new ResponseBuilder(Response.Code.BAD_REQUEST, "Invalid URL").create();
         }

         for(URLFilter filter : topicURLFilters) {
            if(filter.reject(topicURL, request)) {
               return new ResponseBuilder(Response.Code.NOT_FOUND, "The 'hub.topic' is not supported by this server").create();
            }
         }
      }

      String callbackURL = null;

      try {
         callbackURL = urlDecoder.recode(request.getParameterValue("hub.callback"));
      } catch(Exception e) {
         return new ResponseBuilder(Response.Code.BAD_REQUEST, "Invalid URL").create();
      }

      if(callbackURLFilters != null) {
         for(URLFilter filter : callbackURLFilters) {
            if(filter.reject(callbackURL, request)) {
               return new ResponseBuilder(Response.Code.NOT_FOUND, "The 'hub.callback' is not supported by this server").create();
            }
         }
      }

      String callbackHostURL;

      try {
         callbackHostURL = Request.getHostURL(callbackURL);
      } catch(InvalidURIException iue) {
         return new ResponseBuilder(Response.Code.BAD_REQUEST, iue.toString()).create();
      }

      final AuthScheme authScheme;
      final String authId;

      String callbackAuthScheme = request.getParameterValue("hub.x-callback_auth_scheme");
      String callbackAuth = request.getParameterValue("hub.x-callback_auth");

      if(StringUtil.hasContent(callbackAuthScheme) && StringUtil.hasContent(callbackAuth)) {
         try {
            authScheme = datastore.resolveAuthScheme(callbackAuthScheme);
            if(authScheme == null) {
               return new ResponseBuilder(Response.Code.BAD_REQUEST, "Unsupported auth scheme, '" + callbackAuthScheme + "'").create();
            }
            authId = callbackAuth;
         } catch(DatastoreException de) {
            return new ResponseBuilder(Response.Code.SERVER_ERROR, "Internal error").create();
         }
      } else {
         authScheme = null;
         authId = "";
      }

      final SubscriptionVerifier verifier;

      try {
         Subscriber subscriber = datastore.getSubscriber(callbackHostURL, authScheme, authId, true); //Create...
         verifier = verifierFactory.create(request, this, subscriber);
         Response response = verifier.validate();
         if(response != null) { //Error
            return response;
         }
      } catch(DatastoreException de) {
         de.printStackTrace();
         logger.error("Problem getting/creating subscriber", de);
         return new ResponseBuilder(Response.Code.SERVER_ERROR).create();
      }

      verifierService.execute(verifier);
      return new ResponseBuilder(Response.Code.ACCEPTED).create();
   }

   /**
    * Reports the successful completion of subscription verification.
    * @param subscription The verified subscription.
    */
   public void subscriptionVerified(Subscription subscription) {
      subscriptionCallbackMetrics.invalidate(subscription.getId());
   }

   /**
    * Enqueue a verifier for retry after failure.
    * <p>
    * Verify will be retried up to <code>verifyRetry</code> limit after waiting
    * <code>verifyRetryWaitMinutes</code>.
    * </p>
    * @param verifier The verifier.
    * @return Was the verifier enqueued for retry?
    */
   public boolean enqueueVerifierRetry(SubscriptionVerifier verifier) {
      int attempts = verifier.incrementAttempts();
      if(attempts > verifyRetryLimit) {
         return false;
      } else {
         verifierRetryService.schedule(verifier, verifyRetryWaitMinutes, TimeUnit.MINUTES);
         return true;
      }
   }

   /**
    * Enqueue a subscriber callback.
    * @param callback The callback.
    */
   public void enqueueCallback(final Callback callback) {
      callback.incrementAttempts();
      callbackService.submit(callback);
   }

   /**
    * Enqueue a failed subscriber callback.
    * @param callback The callback.
    * @return Was the callback queued?
    */
   public boolean enqueueFailedCallback(final Callback callback) {

      //Track abandoned and failed callbacks to allow failed/offline server heuristic...

      int attempts = callback.incrementAttempts();
      long backoffMillis = failedCallbackRetryStrategy.backoffMillis(attempts);
      if(backoffMillis > 0L) {
         failedCallbackService.schedule(callback, backoffMillis, TimeUnit.MILLISECONDS);
         return true;
      } else {
         maybeDisableSubscription(callback);
         return false;
      }
   }

   /**
    * Possibly disable a failing subscription using a configured strategy.
    * @param callback The callback.
    */
   private void maybeDisableSubscription(final Callback callback) {
      try {
         Subscription subscription = datastore.getSubscription(callback.getSubscriptionId());
         if(subscription != null) {
            SubscriptionCallbackMetrics metrics = subscriptionCallbackMetrics.getUnchecked(callback.getSubscriptionId());
            if(metrics != null && disableSubscriptionStrategy.disableSubscription(subscription,
                    metrics.callbacks, metrics.failedCallbacks, metrics.abandonedCallbacks)) {
               datastore.changeSubscriptionStatus(callback.getSubscriptionId(), Subscription.Status.REMOVED, 0);
               autoDisabledSubscriptions.inc();
               logger.warn("Auto-disabled subscription '" + subscription.callbackURL + "' (" + callback.getSubscriptionId() + ")");
            }
         }
      } catch(DatastoreException de) {
         logger.error("Problem checking subscription for disable", de);
      }
   }

   @Override
   public Map<String, Metric> getMetrics() {
      ImmutableMap.Builder<String, Metric> builder = ImmutableMap.builder();
      builder.putAll(notifierFactory.getMetrics());
      builder.putAll(globalCallbackMetrics.getMetrics());
      //Note: Subscription and host-specific metrics are not included by design!
      builder.putAll(verifierFactory.getMetrics());
      MetricSet datastoreMetrics = datastore.getMetrics();
      if(datastoreMetrics != null) {
         builder.putAll(datastoreMetrics.getMetrics());
      }
      if(notifierServiceQueueSize != null) {
         builder.put("notifier-service-queue-size", notifierServiceQueueSize);
      }
      if(callbackServiceQueueSize != null) {
         builder.put("callback-service-queue-size", callbackServiceQueueSize);
      }
      builder.put("auto-disabled-subscriptions", autoDisabledSubscriptions);
      return builder.build();
   }

   /**
    * Invalidates any internally cached items.
    */
   public void invalidateCaches() {
      notifierFactory.invalidateCaches();
   }

   private final AtomicBoolean isInit = new AtomicBoolean(false);
   private final AtomicBoolean isShutdown = new AtomicBoolean(false);

   private HubDatastore datastore;

   private NotifierFactory notifierFactory;
   private ExecutorService notifierService;
   private CachedGauge<Integer> notifierServiceQueueSize;

   private ExecutorService callbackService;
   private CachedGauge<Integer> callbackServiceQueueSize;

   /**
    * An executor service + runnable that retries failed callbacks by removing them from the
    * failed callback queue and submitting them (again) to the callback service.
    */
   private ScheduledExecutorService failedCallbackService;
   private RetryStrategy failedCallbackRetryStrategy;
   private DisableSubscriptionStrategy disableSubscriptionStrategy;
   private Counter autoDisabledSubscriptions = new Counter();

   /**
    * Gets callback metrics for a subscription.
    * @param subscriptionId The subscription id.
    * @return The meters. If the subscription does not exist, empty meters are returned.
    */
   public SubscriptionCallbackMetrics getSubscriptionCallbackMetrics(final long subscriptionId) {
      return subscriptionCallbackMetrics.getUnchecked(subscriptionId);
   }

   /**
    * Gets combined callback metrics for all subscriptions.
    * @return The callback metrics.
    */
   public CallbackMetrics getGlobalCallbackMetrics() {
      return globalCallbackMetrics;
   }

   /**
    * Gets callback metrics for a specific host.
    * @param host The host.
    * @return The metrics or empty metrics if the host is unknown, or has never been used.
    */
   public HostCallbackMetrics getHostCallbackMetrics(final String host) {
      return hostCallbackMetrics.getUnchecked(host);
   }

   /**
    * Gets the notification metrics for all topics.
    * @return The notification metrics.
    */
   public NotificationMetrics getGlobalNotificationMetrics() {
      return globalNotificationMetrics;
   }

   /**
    * Gets notification metrics for a topic.
    * @param topicId The topic id.
    * @return The metrics or empty metrics if the topic is unknown or has never been used.
    */
   public NotificationMetrics getNotificationMetrics(final long topicId) {
      return notificationMetrics.getUnchecked(topicId);
   }

   /**
    * Gets notification metrics hosts sorted by throughput.
    * @param sort The sort order.
    * @param maxReturned The maximum number returned.
    * @return The list of metrics.
    */
   public List<NotificationMetrics> getNotificationMetrics(final NotificationMetrics.Sort sort, final int maxReturned) {

      if(maxReturned < 1) return Collections.emptyList();

      List<NotificationMetrics> metrics = Lists.newArrayList(notificationMetrics.asMap().values());
      switch(sort) {
         case THROUGHPUT_ASC:
            Collections.sort(metrics, NotificationMetrics.throughputAscendingComparator);
            break;
         case THROUGHPUT_DESC:
            Collections.sort(metrics, Collections.reverseOrder(NotificationMetrics.throughputAscendingComparator));
            break;
      }

      return maxReturned >= metrics.size() ? metrics : metrics.subList(0, maxReturned);
   }

   /**
    * Gets callback metrics hosts sorted by: throughput, failure rate, or abandoned rate.
    * @param sort The sort order.
    * @param maxReturned The maximum number returned.
    * @return The list of metrics.
    */
   public List<HostCallbackMetrics> getHostCallbackMetrics(final CallbackMetrics.Sort sort, final int maxReturned) {

      if(maxReturned < 1) return Collections.emptyList();

      List<HostCallbackMetrics> metrics = Lists.newArrayList(hostCallbackMetrics.asMap().values());
      switch(sort) {
         case THROUGHPUT_ASC:
            Collections.sort(metrics, CallbackMetrics.throughputAscendingComparator);
            break;
         case THROUGHPUT_DESC:
            Collections.sort(metrics, Collections.reverseOrder(CallbackMetrics.throughputAscendingComparator));
            break;
         case FAILURE_RATE_ASC:
            Collections.sort(metrics, CallbackMetrics.failureRateAscendingComparator);
            break;
         case FAILURE_RATE_DESC:
            Collections.sort(metrics, Collections.reverseOrder(CallbackMetrics.failureRateAscendingComparator));
            break;
         case ABANDONED_RATE_ASC:
            Collections.sort(metrics, CallbackMetrics.abandonedRateAscendingComparator);
            break;
         case ABANDONED_RATE_DESC:
            Collections.sort(metrics, Collections.reverseOrder(CallbackMetrics.abandonedRateAscendingComparator));
            break;
      }

      return maxReturned >= metrics.size() ? metrics : metrics.subList(0, maxReturned);
   }

   private int maxMetricsCacheSize = 65536; //TODO: Configure(?)

   /**
    * Callback metrics for all subscriptions.
    */
   final CallbackMetrics globalCallbackMetrics = new CallbackMetrics();

   /**
    * Callback metrics vs subscription id.
    */
   private final LoadingCache<Long, SubscriptionCallbackMetrics> subscriptionCallbackMetrics =
           CacheBuilder.newBuilder()
                   .maximumSize(maxMetricsCacheSize)
                   .concurrencyLevel(8)
                   .build(new CacheLoader<Long, SubscriptionCallbackMetrics>() {
                      @Override
                      public SubscriptionCallbackMetrics load(final Long subscriptionId) throws Exception {
                         return new SubscriptionCallbackMetrics(subscriptionId);
                      }
                   });


   /**
    * Notification metrics for all topics.
    */
   final NotificationMetrics globalNotificationMetrics = new NotificationMetrics(0L);

   /**
    * Notification metrics vs topic id.
    */
   private final LoadingCache<Long, NotificationMetrics> notificationMetrics =
           CacheBuilder.newBuilder()
                   .maximumSize(maxMetricsCacheSize)
                   .concurrencyLevel(8)
                   .build(new CacheLoader<Long, NotificationMetrics>() {
                      @Override
                      public NotificationMetrics load(final Long topicId) throws Exception {
                         return new NotificationMetrics(topicId);
                      }
                   });

   /**
    * Callback metrics vs host.
    */
   private final LoadingCache<String, HostCallbackMetrics> hostCallbackMetrics =
           CacheBuilder.newBuilder()
                   .maximumSize(maxMetricsCacheSize)
                   .concurrencyLevel(8)
                   .build(new CacheLoader<String, HostCallbackMetrics>() {
                      @Override
                      public HostCallbackMetrics load(final String host) throws Exception {
                         return new HostCallbackMetrics(host);
                      }
                   });

   private SubscriptionVerifierFactory verifierFactory;
   private ExecutorService verifierService;
   private ScheduledThreadPoolExecutor verifierRetryService;
   private int verifyRetryWaitMinutes = 10;
   private int verifyRetryLimit = 10;

   private List<URLFilter> topicURLFilters;
   private List<URLFilter> callbackURLFilters;

   private URIEncoder urlDecoder = new URIEncoder();

   private int maxParameterBytes = 1024;
   private String defaultEncoding = "ISO-8859-1";

   private Logger logger;

   private String userAgent;

   private int minLeaseSeconds;
   private int maxLeaseSeconds;

   private Client httpClient;

   private int maxShutdownAwaitSeconds = 30;

   /**
    * A service used to periodically check for expired subscriptions.
    */
   private final ScheduledExecutorService expirationService = Executors.newScheduledThreadPool(1,
           new ThreadFactoryBuilder().setNameFormat("expiration-executor-%d").build());
}
