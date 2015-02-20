/*
 * Copyright 2014 Attribyte, LLC
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

package org.attribyte.api.pubsub.impl.server;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.codahale.metrics.health.HealthCheck;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.codahale.metrics.health.jvm.ThreadDeadlockHealthCheck;
import com.codahale.metrics.servlets.HealthCheckServlet;
import com.codahale.metrics.servlets.MetricsServlet;
import com.codahale.metrics.servlets.ThreadDumpServlet;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.log4j.PropertyConfigurator;
import org.attribyte.api.DatastoreException;
import org.attribyte.api.Logger;
import org.attribyte.api.http.Request;
import org.attribyte.api.http.Response;
import org.attribyte.api.pubsub.BasicAuthFilter;
import org.attribyte.api.pubsub.HubDatastore;
import org.attribyte.api.pubsub.HubEndpoint;
import org.attribyte.api.pubsub.Subscriber;
import org.attribyte.api.pubsub.Subscription;
import org.attribyte.api.pubsub.Topic;
import org.attribyte.api.pubsub.impl.server.admin.AdminAuth;
import org.attribyte.api.pubsub.impl.server.admin.AdminConsole;
import org.attribyte.api.pubsub.impl.server.util.Invalidatable;
import org.attribyte.api.pubsub.impl.server.util.ServerUtil;
import org.attribyte.api.pubsub.impl.server.util.SubscriptionRequestRecord;
import org.attribyte.util.InitUtil;
import org.attribyte.util.StringUtil;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.NCSARequestLog;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.component.LifeCycle;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class Server {

   /**
    * Starts the server.
    * @param args The startup args.
    * @throws Exception on startup error.
    */
   public static void main(String[] args) throws Exception {

      if(args.length < 1) {
         System.err.println("Start-up error: Expecting <config file> [allowed topics file]");
         System.exit(1);
      }

      Properties props = new Properties();
      Properties logProps = new Properties();
      CLI.loadProperties(args, props, logProps);
      final Logger logger = initLogger(props, logProps);

      //Buffer and log hub events for logging and debug...

      final int MAX_STORED_SUBSCRIPTION_REQUESTS = 200;

      final ArrayBlockingQueue<SubscriptionRequestRecord> recentSubscriptionRequests =
              new ArrayBlockingQueue<SubscriptionRequestRecord>(MAX_STORED_SUBSCRIPTION_REQUESTS);

      final HubEndpoint.EventHandler hubEventHandler = new HubEndpoint.EventHandler() {
         private synchronized void offer(SubscriptionRequestRecord record) {
            if(!recentSubscriptionRequests.offer(record)) {
               List<SubscriptionRequestRecord> drain = Lists.newArrayListWithCapacity(MAX_STORED_SUBSCRIPTION_REQUESTS / 2);
               recentSubscriptionRequests.drainTo(drain, drain.size());
               recentSubscriptionRequests.offer(record);
            }
         }

         @Override
         public void subscriptionRequestAccepted(final Request request, final Response response, final Subscriber subscriber) {
            final SubscriptionRequestRecord record;
            try {
               record = new SubscriptionRequestRecord(request, response, subscriber);
            } catch(IOException ioe) {
               return;
            }

            logger.info(record.toString());
            offer(record);
         }

         @Override
         public void subscriptionRequestRejected(final Request request, final Response response, final Subscriber subscriber) {

            final SubscriptionRequestRecord record;
            try {
               record = new SubscriptionRequestRecord(request, response, subscriber);
            } catch(IOException ioe) {
               return;
            }

            logger.warn(record.toString());
            offer(record);
         }
      };

      /**
       * A source for subscription request records (for console, etc).
       */
      final SubscriptionRequestRecord.Source subscriptionRequestRecordSource = new SubscriptionRequestRecord.Source() {
         public List<SubscriptionRequestRecord> latestRequests(int limit) {
            List<SubscriptionRequestRecord> records = Lists.newArrayList(recentSubscriptionRequests);
            Collections.sort(records);
            return records.size() < limit ? records : records.subList(0, limit);
         }
      };

      /**
       * A queue to which new topics are added as reported by the datastore event handler.
       */
      final BlockingQueue<Topic> newTopicQueue = new LinkedBlockingDeque<Topic>();

      /**
       * A datastore event handler that offers new topics to a queue.
       */
      final HubDatastore.EventHandler topicEventHandler = new HubDatastore.EventHandler() {

         @Override
         public void newTopic(final Topic topic) throws DatastoreException {
            newTopicQueue.offer(topic);
         }

         @Override
         public void newSubscription(final Subscription subscription) throws DatastoreException {
            //Ignore
         }

         @Override
         public void exception(final Throwable t) {
            //Ignore
         }

         @Override
         public void setNext(final HubDatastore.EventHandler next) {
            //Ignore
         }
      };

      final HubEndpoint endpoint = new HubEndpoint("endpoint.", props, logger, hubEventHandler, topicEventHandler);

      final String topicAddedTopicURL = Strings.emptyToNull(props.getProperty("endpoint.topicAddedTopic", ""));
      final Topic topicAddedTopic = topicAddedTopicURL != null ? endpoint.getDatastore().getTopic(topicAddedTopicURL, true) : null;
      final Thread topicAddedNotifier = topicAddedTopic != null ? new Thread(new TopicAddedNotifier(newTopicQueue, endpoint, topicAddedTopic)) : null;
      if(topicAddedNotifier != null) {
         topicAddedNotifier.setName("topic-added-notifier");
         topicAddedNotifier.start();
      }

      if(props.getProperty("endpoint.topics") != null) { //Add supported topics...
         for(String topicURL : Splitter.on(",").omitEmptyStrings().trimResults().split(props.getProperty("endpoint.topics"))) {
            Topic topic = endpoint.getDatastore().getTopic(topicURL, true);
            System.out.println("Added topic, '" + topicURL + "' (" + topic.getId() + ")");
         }
      }

      final MetricRegistry registry = new MetricRegistry();
      registry.registerAll(endpoint);

      final HealthCheckRegistry healthCheckRegistry = new HealthCheckRegistry(); //TODO

      final GraphiteReporter reporter;
      String graphiteHost = props.getProperty("graphite.host");
      if(StringUtil.hasContent(graphiteHost)) {
         String graphitePrefix = props.getProperty("graphite.prefix", getHostname()).trim();
         GraphiteReporter.Builder builder = GraphiteReporter.forRegistry(registry);

         int graphitePort = Integer.parseInt(props.getProperty("graphite.port", "2003"));
         long frequencyMillis = InitUtil.millisFromTime(props.getProperty("graphite.frequency", "1m"));
         if(StringUtil.hasContent(graphitePrefix)) {
            builder.prefixedWith(graphitePrefix);
         }
         builder.convertDurationsTo(TimeUnit.MILLISECONDS);
         builder.convertRatesTo(TimeUnit.MINUTES);
         InetSocketAddress addy = new InetSocketAddress(graphiteHost.trim(), graphitePort);
         Graphite graphite = new Graphite(addy);
         reporter = builder.build(graphite);
         reporter.start(frequencyMillis, TimeUnit.MILLISECONDS);
      } else {
         reporter = null;
      }

      String httpAddress = props.getProperty("http.address", "127.0.0.1");
      int httpPort = Integer.parseInt(props.getProperty("http.port", "8086"));

      org.eclipse.jetty.server.Server server = new org.eclipse.jetty.server.Server();

      server.addLifeCycleListener(new LifeCycle.Listener() {

         public void lifeCycleFailure(LifeCycle event, Throwable cause) {
            System.out.println("Failure " + cause.toString());
         }

         public void lifeCycleStarted(LifeCycle event) {
            System.out.println("Started...");
         }

         public void lifeCycleStarting(LifeCycle event) {
            System.out.println("Server Starting...");
         }

         public void lifeCycleStopped(LifeCycle event) {
            System.out.println("Server Stopped...");
         }

         public void lifeCycleStopping(LifeCycle event) {
            if(reporter != null) {
               reporter.stop();
            }
            if(topicAddedNotifier != null) {
               System.out.println("Shutting down new topic notifier...");
               topicAddedNotifier.interrupt();
            }
            System.out.println("Shutting down endpoint...");
            endpoint.shutdown();
            System.out.println("Shutdown endpoint...");
         }
      });

      HttpConfiguration httpConfig = new HttpConfiguration();
      httpConfig.setOutputBufferSize(32768);
      httpConfig.setRequestHeaderSize(8192);
      httpConfig.setResponseHeaderSize(8192);
      httpConfig.setSendServerVersion(false);
      httpConfig.setSendDateHeader(false);
      ServerConnector httpConnector = new ServerConnector(server, new HttpConnectionFactory(httpConfig));
      httpConnector.setHost(httpAddress);
      httpConnector.setPort(httpPort);
      httpConnector.setIdleTimeout(30000L);
      server.addConnector(httpConnector);
      HandlerCollection serverHandlers = new HandlerCollection();
      server.setHandler(serverHandlers);

      ServletContextHandler rootContext = new ServletContextHandler(ServletContextHandler.NO_SESSIONS | ServletContextHandler.NO_SECURITY);
      rootContext.setContextPath("/");

      final AdminConsole adminConsole;
      final List<String> allowedAssetPaths;

      if(props.getProperty("admin.enabled", "false").equalsIgnoreCase("true")) {

         File assetDirFile = getSystemFile("admin.assetDirectory", props);

         if(assetDirFile == null) {
            System.err.println("The 'admin.assetDirectory' must be configured");
            System.exit(1);
         }

         if(!assetDirFile.exists()) {
            System.err.println("The 'admin.assetDirectory'" + assetDirFile.getAbsolutePath() + "' must exist");
            System.exit(1);
         }

         if(!assetDirFile.isDirectory()) {
            System.err.println("The 'admin.assetDirectory'" + assetDirFile.getAbsolutePath() + "' must be a directory");
            System.exit(1);
         }

         if(!assetDirFile.canRead()) {
            System.err.println("The 'admin.assetDirectory'" + assetDirFile.getAbsolutePath() + "' must be readable");
            System.exit(1);
         }

         char[] adminUsername = props.getProperty("admin.username", "").toCharArray();
         char[] adminPassword = props.getProperty("admin.password", "").toCharArray();
         String adminRealm = props.getProperty("admin.realm", "pubsubhub");

         if(adminUsername.length == 0 || adminPassword.length == 0) {
            System.err.println("The 'admin.username' and 'admin.password' must be specified");
            System.exit(1);
         }

         File templateDirFile = getSystemFile("admin.templateDirectory", props);

         if(templateDirFile == null) {
            System.err.println("The 'admin.templateDirectory' must be specified");
            System.exit(1);
         }

         if(!templateDirFile.exists()) {
            System.err.println("The 'admin.templateDirectory'" + assetDirFile.getAbsolutePath() + "' must exist");
            System.exit(1);
         }

         if(!templateDirFile.isDirectory()) {
            System.err.println("The 'admin.templateDirectory'" + assetDirFile.getAbsolutePath() + "' must be a directory");
            System.exit(1);
         }

         if(!templateDirFile.canRead()) {
            System.err.println("The 'admin.templateDirectory'" + assetDirFile.getAbsolutePath() + "' must be readable");
            System.exit(1);
         }

         adminConsole = new AdminConsole(rootContext, assetDirFile.getAbsolutePath(), endpoint,
                 new AdminAuth(adminRealm, adminUsername, adminPassword), templateDirFile.getAbsolutePath(), logger);

         allowedAssetPaths = Lists.newArrayList(
                 Splitter.on(',').omitEmptyStrings().trimResults().split(props.getProperty("admin.assetPaths", ""))
         );
         System.out.println("Admin console is enabled...");
      } else {
         adminConsole = null;
         allowedAssetPaths = ImmutableList.of();
      }

      serverHandlers.addHandler(rootContext);

      //TODO: Introduces incompatible dependency...
      /*
      InstrumentedHandler instrumentedHandler = new InstrumentedHandler(registry);
      instrumentedHandler.setName("http-server");
      instrumentedHandler.setHandler(rootContext);
      serverHandlers.addHandler(instrumentedHandler);
      */

      File requestLogPathFile = getSystemFile("http.log.path", props);
      if(requestLogPathFile != null) {

         if(!requestLogPathFile.exists()) {
            System.err.println("The 'http.log.path', '" + requestLogPathFile.getAbsolutePath() + "' must exist");
            System.exit(1);
         }

         if(!requestLogPathFile.isDirectory()) {
            System.err.println("The 'http.log.path', '" + requestLogPathFile.getAbsolutePath() + "' must be a directory");
            System.exit(1);
         }

         if(!requestLogPathFile.canWrite()) {
            System.err.println("The 'http.log.path', '" + requestLogPathFile.getAbsolutePath() + "' is not writable");
            System.exit(1);
         }

         int requestLogRetainDays = Integer.parseInt(props.getProperty("http.log.retainDays", "14"));
         boolean requestLogExtendedFormat = props.getProperty("http.log.extendedFormat", "true").equalsIgnoreCase("true");
         String requestLogTimeZone = props.getProperty("http.log.timeZone", TimeZone.getDefault().getID());
         String requestLogPrefix = props.getProperty("http.log.prefix", "requests");
         String requestLogPath = requestLogPathFile.getAbsolutePath();
         if(!requestLogPath.endsWith("/")) {
            requestLogPath = requestLogPath + "/";
         }

         NCSARequestLog requestLog = new NCSARequestLog(requestLogPath + requestLogPrefix + "-yyyy_mm_dd.log");
         requestLog.setRetainDays(requestLogRetainDays);
         requestLog.setAppend(true);
         requestLog.setExtended(requestLogExtendedFormat);
         requestLog.setLogTimeZone(requestLogTimeZone);
         requestLog.setLogCookies(false);
         requestLog.setPreferProxiedForAddress(true);

         RequestLogHandler requestLogHandler = new RequestLogHandler();
         requestLogHandler.setRequestLog(requestLog);
         serverHandlers.addHandler(requestLogHandler);
      }

      HubServlet hubServlet = new HubServlet(endpoint, logger);
      rootContext.addServlet(new ServletHolder(hubServlet), "/subscribe/*");

      InitUtil filterInit = new InitUtil("publish.", props);
      List<BasicAuthFilter> publishURLFilters = Lists.newArrayList();
      List<Object> publishURLFilterObjects = filterInit.initClassList("topicURLFilters", BasicAuthFilter.class);
      for(Object o : publishURLFilterObjects) {
         BasicAuthFilter filter = (BasicAuthFilter)o;
         filter.init(filterInit.getProperties());
         publishURLFilters.add(filter);
      }

      final long topicCacheMaxAgeSeconds = Long.parseLong(props.getProperty("endpoint.topicCache.maxAgeSeconds", "0"));
      final Cache<String, Topic> topicCache;
      if(topicCacheMaxAgeSeconds > 0) {
         topicCache = CacheBuilder.newBuilder()
                 .concurrencyLevel(16)
                 .expireAfterWrite(topicCacheMaxAgeSeconds, TimeUnit.SECONDS)
                 .maximumSize(4096)
                 .build();
      } else {
         topicCache = null;
      }

      final String replicationTopicURL = Strings.emptyToNull(props.getProperty("endpoint.replicationTopic", ""));
      //Get or create replication topic, if configured.
      final Topic replicationTopic = replicationTopicURL != null ? endpoint.getDatastore().getTopic(replicationTopicURL, true) : null;

      int maxBodySizeBytes = filterInit.getIntProperty("maxBodySizeBytes", BroadcastServlet.DEFAULT_MAX_BODY_BYTES);
      boolean autocreateTopics = filterInit.getProperty("autocreateTopics", "false").equalsIgnoreCase("true");

      int maxSavedNotifications = filterInit.getIntProperty("maxSavedNotifications", 0);

      final BroadcastServlet broadcastServlet = new BroadcastServlet(endpoint, maxBodySizeBytes, autocreateTopics,
              logger, publishURLFilters, topicCache, replicationTopic, maxSavedNotifications);
      rootContext.addServlet(new ServletHolder(broadcastServlet), "/notify/*");

      CallbackMetricsServlet callbackMetricsServlet = new CallbackMetricsServlet(endpoint);
      ServletHolder callbackMetricsServletHolder = new ServletHolder(callbackMetricsServlet);
      rootContext.addServlet(callbackMetricsServletHolder, "/metrics/callback/*");

      MetricsServlet metricsServlet = new MetricsServlet(registry);
      ServletHolder metricsServletHolder = new ServletHolder(metricsServlet);
      rootContext.setInitParameter(MetricsServlet.RATE_UNIT, "SECONDS");
      rootContext.setInitParameter(MetricsServlet.DURATION_UNIT, "MILLISECONDS");
      rootContext.setInitParameter(MetricsServlet.SHOW_SAMPLES, "false");
      rootContext.addServlet(metricsServletHolder, "/metrics/*");

      PingServlet pingServlet = new PingServlet(props.getProperty("http.instanceName", ""));
      rootContext.addServlet(new ServletHolder(pingServlet), "/ping/*");

      HealthCheckServlet healthCheckServlet = new HealthCheckServlet(healthCheckRegistry);
      for(Map.Entry<String, HealthCheck> healthCheck : endpoint.getDatastore().getHealthChecks().entrySet()) {
         healthCheckRegistry.register(healthCheck.getKey(), healthCheck.getValue());
      }
      healthCheckRegistry.register("no-deadlocked-threads", new ThreadDeadlockHealthCheck());

      rootContext.addServlet(new ServletHolder(healthCheckServlet), "/health/*");

      ThreadDumpServlet threadDumpServlet = new ThreadDumpServlet();
      rootContext.addServlet(new ServletHolder(threadDumpServlet), "/threads/*");

      if(adminConsole != null && allowedAssetPaths.size() > 0) {
         String adminPath = props.getProperty("admin.path", "/admin/");
         List<Invalidatable> invalidatables = Collections.<Invalidatable>singletonList(
                 new Invalidatable() {
                    @Override
                    public void invalidate() {
                       broadcastServlet.invalidateCaches();
                       if(topicCache != null) {
                          topicCache.invalidateAll();
                       }
                    }
                 }
         );
         adminConsole.initServlets(rootContext, adminPath, allowedAssetPaths, invalidatables, broadcastServlet);
      }

      server.setDumpBeforeStop(false);
      server.setStopAtShutdown(true);
      server.start();
      server.join();
   }

   /**
    * Initialize the logger.
    * @param props The main properties.
    * @param logProps The logging properties.
    * @return The logger.
    */
   protected static Logger initLogger(final Properties props, final Properties logProps) {
      PropertyConfigurator.configure(logProps);
      final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(props.getProperty("logger.name", "pubsub"));
      return new Logger() {
         public void debug(final String s) { logger.debug(s); }

         public void info(final String s) { logger.info(s); }

         public void warn(final String s) { logger.warn(s); }

         public void warn(final String s, final Throwable throwable) { logger.warn(s, throwable); }

         public void error(final String s) { logger.error(s); }

         public void error(final String s, final Throwable throwable) { logger.error(s, throwable); }
      };
   }

   /**
    * Gets the hostname.
    * @return The hostname.
    */
   private static String getHostname() {
      try {
         return java.net.InetAddress.getLocalHost().getHostName();
      } catch(UnknownHostException ue) {
         return "[unknown]";
      }
   }

   /**
    * Loads a file defined by a property and expected to be in the system install directory.
    * <p>
    * The system install directory will be added as a prefix if the property value
    * does not start with '/'.
    * </p>
    * @param propName The property name.
    * @param props The properties.
    * @return The file, or <code>null</code> if the property was unspecified.
    */
   private static File getSystemFile(final String propName, final Properties props) {
      String filename = props.getProperty(propName, "").trim();
      if(filename.length() > 0) {
         if(!filename.startsWith("/")) {
            filename = ServerUtil.systemInstallDir() + filename;
         }
         return new File(filename);
      } else {
         return null;
      }
   }
}
