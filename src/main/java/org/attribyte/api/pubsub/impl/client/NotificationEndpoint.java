package org.attribyte.api.pubsub.impl.client;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.servlets.MetricsServlet;
import com.codahale.metrics.servlets.PingServlet;
import com.google.common.base.Optional;
import org.attribyte.api.pubsub.Notification;
import org.attribyte.api.pubsub.Topic;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.HashLoginService;
import org.eclipse.jetty.security.authentication.BasicAuthenticator;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.security.Constraint;
import org.eclipse.jetty.util.security.Credential;

import java.util.Collection;
import java.util.Map;

/**
 * An endpoint that accepts notifications from a hub
 * by listening on a configured address and port.
 * <p>
 * Notifications are reported as they are received to the
 * configured callback.
 * </p>
 */
public class NotificationEndpoint implements MetricSet {

   /**
    * A callback triggered when any notification is received.
    */
   public static interface Callback {

      /**
       * Receive a notification.
       * @param notification The notification.
       */
      public void notification(Notification notification);
   }

   /**
    * Creates an endpoint.
    * @param listenAddress The address to listen on.
    * @param listenPort The port to listen on.
    * @param endpointAuth Optional 'Basic' auth required for calls to the endpoint.
    * @param topics A collection of topics.
    * @param callback The callback.
    */
   public NotificationEndpoint(final String listenAddress,
                               final int listenPort,
                               final Optional<BasicAuth> endpointAuth,
                               final Collection<Topic> topics,
                               final Callback callback) {

      this.server = new org.eclipse.jetty.server.Server();
      HttpConfiguration httpConfig = new HttpConfiguration();
      httpConfig.setOutputBufferSize(1024);
      httpConfig.setRequestHeaderSize(8192);
      httpConfig.setResponseHeaderSize(1024);
      httpConfig.setSendServerVersion(false);
      httpConfig.setSendDateHeader(false);
      ServerConnector httpConnector = new ServerConnector(server, new HttpConnectionFactory(httpConfig));
      httpConnector.setHost(listenAddress);
      httpConnector.setPort(listenPort);
      httpConnector.setIdleTimeout(30000L);
      server.addConnector(httpConnector);
      HandlerCollection serverHandlers = new HandlerCollection();
      server.setHandler(serverHandlers);

      ServletContextHandler rootContext = new ServletContextHandler(ServletContextHandler.NO_SESSIONS | ServletContextHandler.NO_SECURITY);
      rootContext.setContextPath("/");

      PingServlet pingServlet = new PingServlet();
      rootContext.addServlet(new ServletHolder(pingServlet), "/ping/*");

      final NotificationEndpointServlet notificationServlet = new NotificationEndpointServlet(topics, callback);
      this.metrics = notificationServlet.getMetrics();
      rootContext.addServlet(new ServletHolder(notificationServlet), "/*");

      MetricRegistry registry = new MetricRegistry();
      registry.registerAll(notificationServlet);
      MetricsServlet metricsServlet = new MetricsServlet(registry);
      ServletHolder metricsServletHolder = new ServletHolder(metricsServlet);
      rootContext.setInitParameter(MetricsServlet.RATE_UNIT, "SECONDS");
      rootContext.setInitParameter(MetricsServlet.DURATION_UNIT, "MILLISECONDS");
      rootContext.setInitParameter(MetricsServlet.SHOW_SAMPLES, "false");
      rootContext.addServlet(metricsServletHolder, "/metrics/*");

      if(endpointAuth.isPresent()) {
         ConstraintSecurityHandler securityHandler = new ConstraintSecurityHandler();
         HashLoginService loginService = new HashLoginService("pubsub");
         loginService.putUser(endpointAuth.get().username, Credential.getCredential(endpointAuth.get().password), new String[]{"api"});

         Constraint constraint = new Constraint();
         constraint.setName(Constraint.__BASIC_AUTH);
         constraint.setRoles(new String[]{"api"});
         constraint.setAuthenticate(true);

         ConstraintMapping constraintMapping = new ConstraintMapping();
         constraintMapping.setConstraint(constraint);
         constraintMapping.setPathSpec("/*");

         securityHandler.setAuthenticator(new BasicAuthenticator());
         securityHandler.setDenyUncoveredHttpMethods(true);
         securityHandler.setRealmName("endpoint");
         securityHandler.addConstraintMapping(constraintMapping);
         securityHandler.setLoginService(loginService);
         securityHandler.setHandler(rootContext);
         serverHandlers.addHandler(securityHandler);
      } else {
         serverHandlers.addHandler(rootContext);
      }

      server.setStopAtShutdown(true);
   }

   /**
    * Starts the server.
    * @throws Exception on startup error.
    */
   public void start() throws Exception {
      server.start();
   }

   /**
    * Joins the server with the calling thread.
    * @throws Exception on join error.
    */
   public void join() throws Exception {
      server.join();
   }

   /**
    * Stops the server.
    * @throws Exception on stop error.
    */
   public void stop() throws Exception {
      server.stop();
   }

   @Override
   public Map<String, Metric> getMetrics() {
      return metrics;
   }

   private final Server server;
   private final Map<String, Metric> metrics;
}
