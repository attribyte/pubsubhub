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
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.attribyte.api.DatastoreException;
import org.attribyte.api.InitializationException;
import org.attribyte.api.InvalidURIException;
import org.attribyte.api.Logger;
import org.attribyte.api.http.*;
import org.attribyte.util.InitUtil;
import org.attribyte.util.StringUtil;
import org.attribyte.util.URIEncoder;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A pubsubhubub hub.
 * @author Attribyte, LLC
 */
public class HubEndpoint implements MetricSet {
   
   /**
    * Creates an uninitialized endpoint. Required for reflected instantiation.
    * <p>
    *   The <code>init</code> method must be called to initialize the endpoint.
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
    *   The following properties are available. <b>Bold</b> properties are required.
    *   <h2>General</h2>
    *   <dl>
    *     <dt>maxParameterBytes</dt>
    *     <dd>The maximum number of bytes allowed in any parameter. Default is 1024.</dd>
    *     <dt>maxShutdownAwaitSeconds</dt>
    *     <dd>The maximum number of seconds to await for all notifications, callbacks, etc. to complete on
    *     shutdown request. Default 30s.</dd>
    *   </dl>
    *   
    *   <h2>Datastore</h2>
    *   <dl>
    *     <dt><b>datastoreClass</b></dt>
    *     <dd>A class that implements <code>Datastore</code> to provide read/write access to persistent data.</dd>
    *   </dl>
    *   
    *   <h2>HTTP Client</h2>
    *   <dl>  
    *     <dt>httpclient.class<dt>
    *     <dd>The HTTP client implementation. If unspecified, default is <code>org.attribyte.api.http.impl.commons.Client</code>.</dd>
    *     <dt><b>httpclient.userAgent</b></dt>
    *     <dd>The User-Agent string sent with all requests.</dd>
    *     <dt><b>httpclient.connectionTimeoutMillis</b></dt>
    *     <dd>The HTTP connection timeout in milliseconds.</dd>
    *     <dt><b>httpclient.socketTimeoutMillis</b></dt>
    *     <dd>The HTTP client socket timeout in milliseconds.</dd>
    *     <dt>httpclient.proxyHost</dt>
    *     <dd>The HTTP proxy host. If specified, all client requests will use this proxy.</dd>     
    *     <dt>httpclient.proxyPort</dt>
    *     <dd>The HTTP proxy port. Required when <code>proxyHost</code> is specified</dd> 
    *   </dl>
    *   
    *   <h2>Notifications</h2>
    *   
    *   <h3>Notifiers</h3>
    *   <dl>  
    *     <dt><b>notifierFactoryClass</b></dt>
    *     <dd>Implementation of <code>NotifierFactory</code>. Creates instances of (<code>Runnable</code>) <code>Notifier</code>
    *     used to schedule send of <code>Notification</code> to all subscribers.</dd>
    *     <dt><b>maxConcurrentNotifiers</b></dt>
    *     <dd>The maximum number of concurrent notifiers.</dd> 
    *     <dt>baseConcurrentNotifiers</dt>
    *     <dd>The minimum number of threads waiting to execute notifiers.</dd> 
    *     <dt>maxNotifierQueueSize</dt>
    *     <dd>The maximum number of notifiers queued when all threads are busy.</dd> 
    *     <dt>notifierThreadKeepAliveMinutes</dt>
    *     <dd>The number of minutes notifier threads remain idle.</dd>  
    *     <dt>notifierExecutorServiceClass</dt>
    *     <dd>A user-defined service for executing notifiers.
    *         Must implement <code>ExecutorService</code> and have a default initializer.
    *     </dd>
    *   </dl>
    *   
    *   <h3>Subscriber Callback</h3>
    *   <dl>  
    *     <dt>callbackFactoryClass</dt>
    *     <dd>Implementation of <code>CallbackFactory</code>. Creates instances of (<code>Runnable</code>) <code>Callback</code>
    *     used to callback to all subscribers.</dd>   
    *     <dt><b>maxConcurrentCallbacks</b></dt>
    *     <dd>The maximum number of concurrent callbacks.</dd> 
    *     <dt>callbackThreadKeepAliveMinutes</dt>
    *     <dd>The number of minutes callback threads remain idle.</dd>  
    *     <dt>callbackExecutorServiceClass</dt>
    *     <dd>A user-defined service for executing callback.
    *         Must implement <code>ExecutorService</code> and have a default initializer.
    *     </dd>
    *   </dl>
    *   
    *   <h2>Subscriptions</h2>
    *   <dl>
    *     <dt><b>verifierFactoryClass</b></dt>
    *     <dd>Implementation of <code>VerifierFactory</code>. Creates instances of (<code>Runnable</code>) <code>Verifier</code>
    *     </dd>
    *     <dt><b>maxConcurrentVerifiers</b></dt>
    *     <dd>The maximum number of concurrent subscription verifiers.</dd> 
    *     <dt>baseConcurrentVerifiers</dt>
    *     <dd>The minimum number of threads waiting to verify subscriptions.</dd> 
    *     <dt>maxVerifierQueueSize</dt>
    *     <dd>The maximum number of subscription verifications queued when all callback threads are busy.</dd> 
    *     <dt>verifierThreadKeepAliveMinutes</dt>
    *     <dd>The number of minutes subscription verifier threads remain idle.</dd>  
    *     <dt>verifierExecutorServiceClass</dt>
    *     <dd>A user-defined executor service for subscription verification.
    *         Must implement <code>ExecutorService</code> and have a default initializer.
    *     </dd>
    *     <dt>verifyRetryWaitMinutes</dt>
    *     <dd>The minimum number of minutes before (async) verify retry if initial verify fails.
    *         Default is 10 minutes.
    *     </dd>
    *     <dt>verifyRetryLimit</dd>
    *     <dd>The maximum number of verify retries. Default 10.</dd>
    *     <dt><b>verifyRetryThreads</b></dt>
    *     <dd>The number of threads available to handle verify retry.</dd>
    *     <dt>topicURLFilters</dt>
    *     <dd>A space (or comma) separated list of fully-qualified <code>URLFilters</code> to be applied to the topic
    *     URL of any subscriptions. Filters are applied, in the order they appear, before any subscription processing.</dd>
    *     <dt>callbackURLFilters</dt>
    *     <dd>A space (or comma) separated list of fully-qualified <code>URLFilters</code> to be applied to the callback
    *     URL of any subscriptions. Filters are applied, in the order they appear, before any subscription processing.</dd>     
    *     <dt><b>minLeaseSeconds</b></dt>
    *     <dd>The minimum allowed lease time.</dd>
    *     <dt><b>maxLeaseSeconds</b></dt>
    *     <dd>The maximum allowed lease time.</dd>     
    *  </dl>
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
         httpClient.init(prefix+"httpclient.", props, logger);

         notifierFactory = (NotifierFactory)initUtil.initClass("notifierFactoryClass", NotifierFactory.class);
         if(notifierFactory == null) {
            initUtil.throwRequiredException("notifierFactoryClass");
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

         callbackFactory = (CallbackFactory)initUtil.initClass("callbackFactoryClass", CallbackFactory.class);
         if(callbackFactory == null) {
            initUtil.throwRequiredException("callbackFactoryClass");
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

            final BlockingQueue queue = new LinkedBlockingQueue();
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

         verifierFactory = (SubscriptionVerifierFactory)initUtil.initClass("verifierFactoryClass", SubscriptionVerifierFactory.class);
         if(verifierFactory == null) {
            initUtil.throwRequiredException("verifierFactoryClass");
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

         List<Object> callbackURLFilterObjects = initUtil.initClassList("callbackURLFilters", URLFilter.class);
         if(callbackURLFilterObjects.size() > 0) {
            callbackURLFilters = new ArrayList<URLFilter>(callbackURLFilterObjects.size() + 1);
            callbackURLFilters.add(new FragmentRejectFilter());
            for(Object o : callbackURLFilterObjects) {
               callbackURLFilters.add((URLFilter)o);
            }
            callbackURLFilters = Collections.unmodifiableList(callbackURLFilters);
         }  else {
            callbackURLFilters = new ArrayList<URLFilter>(1);
            callbackURLFilters.add(new FragmentRejectFilter());
            callbackURLFilters = Collections.unmodifiableList(callbackURLFilters);
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

            logger.info("Shutting down notifier service...");
            long startMillis = System.currentTimeMillis();
            notifierService.shutdown();
            boolean terminatedNormally = notifierService.awaitTermination(maxShutdownAwaitSeconds, TimeUnit.SECONDS);
            long elapsedMillis = System.currentTimeMillis() - startMillis;
            if(terminatedNormally) {
               logger.info("Notifier service shutdown normally in "+elapsedMillis+" ms.");
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
                logger.info("Callback service shutdown normally in "+elapsedMillis+" ms.");
             } else {
                callbackService.shutdownNow();
                logger.info("Callback service shutdown *abnormally* in "+elapsedMillis+" ms.");
             }

            logger.info("Shutting down verifier service...");
            startMillis = System.currentTimeMillis();
            verifierService.shutdown();
            terminatedNormally = verifierService.awaitTermination(maxShutdownAwaitSeconds, TimeUnit.SECONDS);
            elapsedMillis = System.currentTimeMillis() - startMillis;
            if(terminatedNormally) {
               logger.info("Verifier service shutdown normally in "+elapsedMillis+" ms.");
            } else {
               verifierService.shutdownNow();
               logger.info("Verifier service shutdown *abnormally* in "+elapsedMillis+" ms.");
            }

            logger.info("Shutting down verifier retry service...");
            startMillis = System.currentTimeMillis();
            verifierRetryService.shutdown();
            terminatedNormally = verifierRetryService.awaitTermination(maxShutdownAwaitSeconds, TimeUnit.SECONDS);
            elapsedMillis = System.currentTimeMillis() - startMillis;
            if(terminatedNormally) {
               logger.info("Verifier retry service shutdown normally in "+elapsedMillis+" ms.");
            } else {
               verifierRetryService.shutdownNow();
               logger.info("Verifier retry service shutdown *abnormally* in "+elapsedMillis+" ms.");
            }

            logger.info("Shutting down failed callback service...");
            startMillis = System.currentTimeMillis();
            failedCallbackService.shutdown();
            terminatedNormally = failedCallbackService.awaitTermination(maxShutdownAwaitSeconds, TimeUnit.SECONDS);
            elapsedMillis = System.currentTimeMillis() - startMillis;
            if(terminatedNormally) {
               logger.info("Failed callback service shutdown normally in "+elapsedMillis+" ms.");
            } else {
               failedCallbackService.shutdownNow();
               logger.info("Failed callback service shutdown *abnormally* in "+elapsedMillis+" ms.");
            }
         } catch(InterruptedException ie) {
            Thread.currentThread().interrupt();
         }

         httpClient.shutdown();
         datastore.shutdown();

         logger.info("Endpoint shutdown complete.");
      }
   }
   
   /**
    * Enqueue a new content notification.
    * <p>
    *    A notification identifies new content for a topic. When
    *    the notification is processed, all topic subscribers are notified
    *    at their callback URL.
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
            if(filter.reject(topicURL)) {
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
            if(filter.reject(callbackURL)) {
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
               return new ResponseBuilder(Response.Code.BAD_REQUEST, "Unsupported auth scheme, '"+callbackAuthScheme+"'").create();
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
    * Enqueue a verifier for retry after failure.
    * <p>
    *    Verify will be retried up to <code>verifyRetry</code> limit after waiting
    *    <code>verifyRetryWaitMinutes</code>.
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
    * Enqueue a callback with a specified subscriber id and priority.
    * @param request The request.
    * @param subscriberId The subscriber id.
    * @param priority The priority.
    */
   public void enqueueCallback(Request request, long subscriberId, int priority) {
      enqueueCallback(callbackFactory.create(request, subscriberId, priority, this));
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
      int attempts = callback.incrementAttempts();
      if(attempts < 128) { //TODO: Confiugure all this...
         failedCallbackService.schedule(callback, attempts * 30, TimeUnit.SECONDS);
         return true;
      } else {
         return false;
      }
   }

   @Override
   public Map<String, Metric> getMetrics() {
      ImmutableMap.Builder<String, Metric> builder = ImmutableMap.builder();
      builder.putAll(notifierFactory.getMetrics());
      builder.putAll(callbackFactory.getMetrics());
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
      return builder.build();
   }

   private final AtomicBoolean isInit = new AtomicBoolean(false);
   private final AtomicBoolean isShutdown = new AtomicBoolean(false);

   private HubDatastore datastore;

   private NotifierFactory notifierFactory;
   private ExecutorService notifierService;
   private CachedGauge<Integer> notifierServiceQueueSize;

   private CallbackFactory callbackFactory;
   private ExecutorService callbackService;
   private CachedGauge<Integer> callbackServiceQueueSize;

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
    * An executor service + runnable that retries failed callbacks by removing them from the
    * failed callback queue and submitting them (again) to the callback service.
    */
   private final ScheduledExecutorService failedCallbackService = Executors.newScheduledThreadPool(4,
           new ThreadFactoryBuilder().setNameFormat("failed-callback-executor-%d").build()); //TODO: Configure

   /**
    * A service used to periodically check for expired subscriptions.
    */
   private final ScheduledExecutorService expirationService = Executors.newScheduledThreadPool(1,
           new ThreadFactoryBuilder().setNameFormat("expiration-executor-%d").build());
}
