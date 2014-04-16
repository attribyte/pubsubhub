package org.attribyte.api.pubsub.impl.server;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.codahale.metrics.health.HealthCheck;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.codahale.metrics.health.jvm.ThreadDeadlockHealthCheck;
import com.codahale.metrics.servlets.HealthCheckServlet;
import com.codahale.metrics.servlets.MetricsServlet;
import com.codahale.metrics.servlets.PingServlet;
import com.codahale.metrics.servlets.ThreadDumpServlet;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.log4j.PropertyConfigurator;
import org.attribyte.api.Logger;
import org.attribyte.api.pubsub.BasicAuthFilter;
import org.attribyte.api.pubsub.HubEndpoint;
import org.attribyte.api.pubsub.Topic;
import org.attribyte.util.InitUtil;
import org.attribyte.util.StringUtil;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.component.LifeCycle;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

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
      loadProperties(args, props, logProps);

      final Logger logger = initLogger(props, logProps);
      final HubEndpoint endpoint = new HubEndpoint("endpoint.", props, logger, null);

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
      serverHandlers.addHandler(rootContext);

      //TODO: Introduces incompatible dependency...
      /*
      InstrumentedHandler instrumentedHandler = new InstrumentedHandler(registry);
      instrumentedHandler.setName("http-server");
      instrumentedHandler.setHandler(rootContext);
      serverHandlers.addHandler(instrumentedHandler);
      */

      /*
      NCSARequestLog requestLog = new NCSARequestLog(requestLogPath+requestLogBase+"-yyyy_mm_dd.request.log");
      requestLog.setRetainDays(requestLogRetainDays);
      requestLog.setAppend(true);
      requestLog.setExtended(requestLogExtendedFormat);
      requestLog.setLogTimeZone(requestLogTimeZone);
      requestLog.setLogCookies(false);

      RequestLogHandler requestLogHandler = new RequestLogHandler();
      requestLogHandler.setRequestLog(requestLog);
      serverHandlers.addHandler(requestLogHandler);
      */

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

      BroadcastServlet broadcastServlet = new BroadcastServlet(endpoint, logger, publishURLFilters);
      rootContext.addServlet(new ServletHolder(broadcastServlet), "/notify/*");

      MetricsServlet metricsServlet = new MetricsServlet(registry);
      ServletHolder metricsServletHolder = new ServletHolder(metricsServlet);
      rootContext.setInitParameter(MetricsServlet.RATE_UNIT, "SECONDS");
      rootContext.setInitParameter(MetricsServlet.DURATION_UNIT, "MILLISECONDS");
      rootContext.setInitParameter(MetricsServlet.SHOW_SAMPLES, "false");
      rootContext.addServlet(metricsServletHolder, "/metrics/*");

      PingServlet pingServlet = new PingServlet();
      rootContext.addServlet(new ServletHolder(pingServlet), "/ping/*");

      HealthCheckServlet healthCheckServlet = new HealthCheckServlet(healthCheckRegistry);
      for(Map.Entry<String, HealthCheck> healthCheck : endpoint.getDatastore().getHealthChecks().entrySet()) {
         healthCheckRegistry.register(healthCheck.getKey(), healthCheck.getValue());
      }
      healthCheckRegistry.register("no-deadlocked-threads", new ThreadDeadlockHealthCheck());

      rootContext.addServlet(new ServletHolder(healthCheckServlet), "/health/*");

      ThreadDumpServlet threadDumpServlet = new ThreadDumpServlet();
      rootContext.addServlet(new ServletHolder(threadDumpServlet), "/threads/*");

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
   protected static final Logger initLogger(final Properties props, final Properties logProps) {
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

   protected static final void loadProperties(final String[] filenames, final Properties props, final Properties logProps) {

      for(String filename : filenames) {

         File f = new File(filename);

         if(!f.exists()) {
            System.err.println("Start-up error: The configuration file, '" + f.getAbsolutePath() + " does not exist");
            System.exit(0);
         }

         if(!f.canRead()) {
            System.err.println("Start-up error: The configuration file, '" + f.getAbsolutePath() + " is not readable");
            System.exit(0);
         }

         FileInputStream fis = null;
         Properties currProps = new Properties();

         try {
            fis = new FileInputStream(f);
            currProps.load(fis);
            if(f.getName().startsWith("log.")) {
               logProps.putAll(currProps);
            } else {
               props.putAll(currProps);
            }
         } catch(IOException ioe) {
            //TODO
         } finally {
            try {
               fis.close();
            } catch(Exception e) {
               //TODO
            }
         }
      }
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

}
