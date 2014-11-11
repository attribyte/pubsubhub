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


package org.attribyte.api.pubsub.impl.server.admin;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.attribyte.api.Logger;
import org.attribyte.api.http.AuthScheme;
import org.attribyte.api.http.impl.BasicAuthScheme;
import org.attribyte.api.pubsub.CallbackMetrics;
import org.attribyte.api.pubsub.HostCallbackMetrics;
import org.attribyte.api.pubsub.HubDatastore;
import org.attribyte.api.pubsub.HubEndpoint;
import org.attribyte.api.pubsub.NotificationMetrics;
import org.attribyte.api.pubsub.Subscriber;
import org.attribyte.api.pubsub.Subscription;
import org.attribyte.api.pubsub.Topic;
import org.attribyte.api.pubsub.impl.server.admin.model.DisplayCallbackMetrics;
import org.attribyte.api.pubsub.impl.server.admin.model.DisplayCallbackMetricsDetail;
import org.attribyte.api.pubsub.impl.server.admin.model.DisplayNotificationMetrics;
import org.attribyte.api.pubsub.impl.server.admin.model.DisplayNotificationMetricsDetail;
import org.attribyte.api.pubsub.impl.server.admin.model.DisplaySubscribedHost;
import org.attribyte.api.pubsub.impl.server.admin.model.DisplayTopic;
import org.attribyte.api.pubsub.impl.server.admin.model.Paging;
import org.attribyte.util.URIEncoder;
import org.stringtemplate.v4.DateRenderer;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STErrorListener;
import org.stringtemplate.v4.STGroup;
import org.stringtemplate.v4.STGroupDir;
import org.stringtemplate.v4.STGroupFile;
import org.stringtemplate.v4.misc.STMessage;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.attribyte.api.pubsub.impl.server.util.ServletUtil.splitPath;

@SuppressWarnings("serial")
/**
 * Implements the browser-based administrative console.
 */
public class AdminServlet extends HttpServlet {

   public AdminServlet(final HubEndpoint endpoint,
                       final AdminAuth auth,
                       final String templateDirectory,
                       final Logger logger) {
      this.endpoint = endpoint;
      this.datastore = endpoint.getDatastore();
      this.auth = auth;
      this.templateGroup = new STGroupDir(templateDirectory, '$', '$');
      this.templateGroup.setListener(new ErrorListener());
      this.templateGroup.registerRenderer(java.util.Date.class, new DateRenderer());

      File globalConstantsFile = new File(templateDirectory, "constants.stg");
      STGroupFile globalConstants = null;
      if(globalConstantsFile.exists()) {
         globalConstants = new STGroupFile(globalConstantsFile.getAbsolutePath());
         this.templateGroup.importTemplates(globalConstants);
      }

      this.logger = logger;
   }

   public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
      if(!auth.authIsValid(request, response)) return;
      List<String> path = splitPath(request);
      String obj = path.size() > 0 ? path.get(0) : null;
      if(obj != null) {
         if(obj.equals("subscription")) {
            postSubscriptionEdit(request, path.size() > 1 ? path.get(1) : null, response);
         } else if(obj.equals("topic")) {
            postTopicAdd(request, response);
         } else {
            sendNotFound(response);
         }
      } else {
         sendNotFound(response);
      }
   }

   public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
      if(!auth.authIsValid(request, response)) return;
      List<String> path = splitPath(request);
      String obj = path.size() > 0 ? path.get(0) : null;

      if(obj == null || obj.equals("topics")) {
         boolean activeOnly = obj == null || path.size() > 1 && path.get(1).equals("active");
         renderTopics(request, activeOnly, response);
      } else if(obj.equals("subscribers")) {
         renderSubscribers(request, response);
      } else if(obj.equals("topic")) {
         if(path.size() > 1) {
            boolean activeOnly = path.size() > 2 && path.get(2).equals("active");
            renderTopicSubscriptions(path.get(1), request, activeOnly, response);
         } else {
            sendNotFound(response);
         }
      } else if(obj.equals("host")) {
         if(path.size() > 1) {
            boolean activeOnly = path.size() > 2 && path.get(2).equals("active");
            renderHostSubscriptions(path.get(1), request, activeOnly, response);
         } else {
            sendNotFound(response);
         }
      } else if(obj.equals("subscriptions")) {
         boolean activeOnly = path.size() > 1 && path.get(1).equals("active");
         renderAllSubscriptions(request, activeOnly, response);
      } else if(obj.equals("metrics")) {
         if(path.size() > 1) {
            renderCallbackMetricsDetail(request, path.get(1), response);
         } else {
            renderCallbackMetrics(request, response);
         }
      } else if(obj.equals("nmetrics")) {
         if(path.size() > 1) {
            long topicId = Long.parseLong(path.get(1));
            renderNotificationMetricsDetail(request, topicId, response);
         } else {
            renderNotificationMetrics(request, response);
         }
      } else {
         sendNotFound(response);
      }
   }

   private void renderSubscribers(final HttpServletRequest request,
                                  final HttpServletResponse response) throws IOException {

      ST mainTemplate = getTemplate("main");
      ST subscriberTemplate = getTemplate("subscribers");

      try {
         Paging paging = getPaging(request);
         List<DisplaySubscribedHost> subscribers = Lists.newArrayListWithExpectedSize(25);
         List<String> endpoints = datastore.getSubscribedHosts(paging.getStart(), pageRequestSize);
         paging = nextPaging(paging, endpoints);
         for(String host : endpoints) {
            subscribers.add(new DisplaySubscribedHost(host, datastore.countActiveHostSubscriptions(host)));
         }
         subscriberTemplate.add("subscribers", subscribers);
         subscriberTemplate.add("paging", paging);
         mainTemplate.add("content", subscriberTemplate.render());
         response.setContentType("text/html");
         response.getWriter().print(mainTemplate.render());
         response.getWriter().flush();
      } catch(Exception se) {
         se.printStackTrace();
         response.sendError(500, "Datastore error");
      }
   }

   private void renderTopics(final HttpServletRequest request,
                             boolean activeOnly,
                             final HttpServletResponse response) throws IOException {

      ST mainTemplate = getTemplate("main");
      ST subscriberTemplate = getTemplate("topics");

      try {

         Paging paging = getPaging(request);
         List<DisplayTopic> displayTopics = Lists.newArrayListWithExpectedSize(25);
         List<Topic> topics = activeOnly ? datastore.getActiveTopics(paging.getStart(), pageRequestSize) :
                 datastore.getTopics(paging.getStart(), pageRequestSize);
         paging = nextPaging(paging, topics);
         for(Topic topic : topics) {
            displayTopics.add(new DisplayTopic(topic, datastore.countActiveSubscriptions(topic.getId()),
                    new DisplayNotificationMetrics(topic, endpoint.getNotificationMetrics(topic.getId()))));
         }

         subscriberTemplate.add("topics", displayTopics);
         subscriberTemplate.add("activeOnly", activeOnly);
         subscriberTemplate.add("paging", paging);
         mainTemplate.add("content", subscriberTemplate.render());
         response.setContentType("text/html");
         response.getWriter().print(mainTemplate.render());
         response.getWriter().flush();
      } catch(Exception se) {
         se.printStackTrace();
         response.sendError(500, "Datastore error");
      }
   }


   private Set<Subscription.Status> getSubscriptionStatus(HttpServletRequest request, final boolean activeOnly) {

      if(activeOnly) {
         return Collections.singleton(Subscription.Status.ACTIVE);
      }

      String[] status = request.getParameterValues("status");
      if(status == null || status.length == 0) {
         return Collections.emptySet();
      } else {
         Set<Subscription.Status> statusSet = Sets.newHashSetWithExpectedSize(4);
         for(String s : status) {
            Subscription.Status toAdd = Subscription.Status.valueOf(s);
            if(toAdd != Subscription.Status.INVALID) {
               statusSet.add(toAdd);
            }
         }

         return statusSet;
      }
   }

   private void renderTopicSubscriptions(final String topicIdStr, final HttpServletRequest request,
                                         final boolean activeOnly,
                                         final HttpServletResponse response) throws IOException {

      ST mainTemplate = getTemplate("main");
      ST subscriptionsTemplate = getTemplate("topic_subscriptions");

      try {
         long topicId = Long.parseLong(topicIdStr);
         Topic topic = datastore.getTopic(topicId);
         if(topic == null) {
            sendNotFound(response);
            return;
         }

         Paging paging = getPaging(request);
         List<Subscription> subscriptions = datastore.getTopicSubscriptions(topic, getSubscriptionStatus(request, activeOnly),
                 paging.getStart(), pageRequestSize);
         paging = nextPaging(paging, subscriptions);

         subscriptionsTemplate.add("subscriptions", subscriptions);
         subscriptionsTemplate.add("topic", new DisplayTopic(topic, 0, null));
         subscriptionsTemplate.add("activeOnly", activeOnly);
         subscriptionsTemplate.add("paging", paging);
         mainTemplate.add("content", subscriptionsTemplate.render());
         response.setContentType("text/html");
         response.getWriter().print(mainTemplate.render());
         response.getWriter().flush();
      } catch(NumberFormatException nfe) {
         response.sendError(400, "Invalid topic id");
      } catch(Exception se) {
         se.printStackTrace();
         response.sendError(500, "Datastore error");
      }
   }

   private void renderHostSubscriptions(final String host, final HttpServletRequest request,
                                        final boolean activeOnly,
                                        final HttpServletResponse response) throws IOException {

      ST mainTemplate = getTemplate("main");
      ST subscriptionsTemplate = getTemplate("host_subscriptions");

      try {
         Paging paging = getPaging(request);
         List<Subscription> subscriptions = datastore.getHostSubscriptions(host, getSubscriptionStatus(request, activeOnly),
                 paging.getStart(), pageRequestSize);
         paging = nextPaging(paging, subscriptions);

         subscriptionsTemplate.add("subscriptions", subscriptions);
         subscriptionsTemplate.add("host", host);
         subscriptionsTemplate.add("activeOnly", activeOnly);
         subscriptionsTemplate.add("paging", paging);
         mainTemplate.add("content", subscriptionsTemplate.render());
         response.setContentType("text/html");
         response.getWriter().print(mainTemplate.render());
         response.getWriter().flush();
      } catch(NumberFormatException nfe) {
         response.sendError(400, "Invalid topic id");
      } catch(Exception se) {
         se.printStackTrace();
         response.sendError(500, "Datastore error");
      }
   }

   private void renderAllSubscriptions(final HttpServletRequest request,
                                       final boolean activeOnly,
                                       final HttpServletResponse response) throws IOException {

      ST mainTemplate = getTemplate("main");
      ST subscriptionsTemplate = getTemplate("all_subscriptions");

      try {

         Paging paging = getPaging(request);
         List<Subscription> subscriptions = datastore.getSubscriptions(getSubscriptionStatus(request, activeOnly),
                 paging.getStart(), pageRequestSize);
         paging = nextPaging(paging, subscriptions);

         subscriptionsTemplate.add("subscriptions", subscriptions);
         subscriptionsTemplate.add("activeOnly", activeOnly);
         subscriptionsTemplate.add("paging", paging);
         mainTemplate.add("content", subscriptionsTemplate.render());
         response.setContentType("text/html");
         response.getWriter().print(mainTemplate.render());
         response.getWriter().flush();
      } catch(NumberFormatException nfe) {
         response.sendError(400, "Invalid topic id");
      } catch(IOException ioe) {
         throw ioe;
      } catch(Exception se) {
         se.printStackTrace();
         response.sendError(500, "Datastore error");
      }
   }

   private Paging getPaging(final HttpServletRequest request) {
      String pageStr = request.getParameter("p");
      if(pageStr == null || pageStr.trim().length() == 0) {
         pageStr = "1";
      }
      return new Paging(Integer.parseInt(pageStr), maxPerPage, false);
   }

   /**
    * Expects the input list to have at least one additional object that is
    * used to determine if there are more pages.
    * @param currPage The current page.
    * @param pageList The list of objects.
    * @return The new paging.
    */
   private Paging nextPaging(final Paging currPage, final List<? extends Object> pageList) {
      if(pageList.size() > maxPerPage) {
         while(pageList.size() > maxPerPage) pageList.remove(pageList.size() - 1);
         return new Paging(currPage.getCurr(), maxPerPage, true);
      } else {
         return new Paging(currPage.getCurr(), maxPerPage, false);
      }
   }

   private void renderCallbackMetrics(final HttpServletRequest request,
                                      final HttpServletResponse response) throws IOException {

      ST mainTemplate = getTemplate("main");
      ST metricsTemplate = getTemplate("callback_metrics");

      try {

         CallbackMetrics globalMetrics = endpoint.getGlobalCallbackMetrics();
         List<HostCallbackMetrics> metrics = endpoint.getHostCallbackMetrics(CallbackMetrics.Sort.THROUGHPUT_DESC, 25);

         List<DisplayCallbackMetrics> displayMetrics = Lists.newArrayListWithCapacity(metrics.size() + 1);
         displayMetrics.add(new DisplayCallbackMetrics("[all]", globalMetrics));
         for(HostCallbackMetrics hcm : metrics) {
            displayMetrics.add(new DisplayCallbackMetrics(hcm.host, hcm));
         }
         metricsTemplate.add("metrics", displayMetrics);
         mainTemplate.add("content", metricsTemplate.render());
         response.setContentType("text/html");
         response.getWriter().print(mainTemplate.render());
         response.getWriter().flush();
      } catch(IOException ioe) {
         throw ioe;
      } catch(Exception se) {
         se.printStackTrace();
         response.sendError(500, "Datastore error");
      }
   }

   private void renderCallbackMetricsDetail(final HttpServletRequest request,
                                            final String hostOrId,
                                            final HttpServletResponse response) throws IOException {

      ST mainTemplate = getTemplate("main");
      ST metricsTemplate = getTemplate("metrics_detail");

      try {
         final CallbackMetrics detailMetrics;
         final String title;

         long subscriptionId = 0L;
         if(hostOrId != null) {
            try {
               subscriptionId = Long.parseLong(hostOrId);
            } catch(NumberFormatException ne) {
               subscriptionId = 0L;
            }
         }

         final String host = hostOrId;
         if(subscriptionId > 0) {
            Subscription subscription = datastore.getSubscription(subscriptionId);
            if(subscription != null) {
               title = subscription.getCallbackURL();
               detailMetrics = endpoint.getSubscriptionCallbackMetrics(subscriptionId);
            } else {
               sendNotFound(response);
               return;
            }
         } else if(host == null || host.equalsIgnoreCase("[all]")) {
            title = "All Hosts";
            detailMetrics = endpoint.getGlobalCallbackMetrics();
         } else {
            title = host;
            detailMetrics = endpoint.getHostCallbackMetrics(host);
         }
         metricsTemplate.add("metrics", new DisplayCallbackMetricsDetail(title, detailMetrics));
         mainTemplate.add("content", metricsTemplate.render());
         response.setContentType("text/html");
         response.getWriter().print(mainTemplate.render());
         response.getWriter().flush();
      } catch(IOException ioe) {
         throw ioe;
      } catch(Exception se) {
         se.printStackTrace();
         response.sendError(500, "Datastore error");
      }
   }

   private void renderNotificationMetrics(final HttpServletRequest request,
                                          final HttpServletResponse response) throws IOException {

      ST mainTemplate = getTemplate("main");
      ST metricsTemplate = getTemplate("notification_metrics");

      try {
         NotificationMetrics globalMetrics = endpoint.getGlobalNotificationMetrics();
         List<NotificationMetrics> metrics = endpoint.getNotificationMetrics(NotificationMetrics.Sort.THROUGHPUT_DESC, 25);

         List<DisplayNotificationMetrics> displayMetrics = Lists.newArrayListWithCapacity(metrics.size() + 1);
         displayMetrics.add(new DisplayNotificationMetrics(null, globalMetrics));
         for(NotificationMetrics topicMetrics : metrics) {
            Topic topic = datastore.getTopic(topicMetrics.topicId);
            displayMetrics.add(new DisplayNotificationMetrics(topic, topicMetrics));
         }
         metricsTemplate.add("metrics", displayMetrics);
         mainTemplate.add("content", metricsTemplate.render());
         response.setContentType("text/html");
         response.getWriter().print(mainTemplate.render());
         response.getWriter().flush();
      } catch(IOException ioe) {
         throw ioe;
      } catch(Exception se) {
         se.printStackTrace();
         response.sendError(500, "Datastore error");
      }
   }

   private void renderNotificationMetricsDetail(final HttpServletRequest request,
                                                final long topicId,
                                                final HttpServletResponse response) throws IOException {

      ST mainTemplate = getTemplate("main");
      ST metricsTemplate = getTemplate("notification_metrics_detail");

      try {
         final NotificationMetrics detailMetrics;
         final String title;

         if(topicId > 0) {
            Topic topic = datastore.getTopic(topicId);
            if(topic != null) {
               title = topic.getURL();
               detailMetrics = endpoint.getNotificationMetrics(topicId);
            } else {
               sendNotFound(response);
               return;
            }
         } else {
            title = "[all]";
            detailMetrics = endpoint.getGlobalNotificationMetrics();
         }
         metricsTemplate.add("metrics", new DisplayNotificationMetricsDetail(title, detailMetrics));
         mainTemplate.add("content", metricsTemplate.render());
         response.setContentType("text/html");
         response.getWriter().print(mainTemplate.render());
         response.getWriter().flush();
      } catch(IOException ioe) {
         throw ioe;
      } catch(Exception se) {
         se.printStackTrace();
         response.sendError(500, "Datastore error");
      }
   }

   private void postTopicAdd(final HttpServletRequest request, final HttpServletResponse response) throws IOException {
      String url = request.getParameter("url");
      if(url == null || url.trim().length() == 0) {
         response.sendError(400);
         return;
      }

      try {
         Topic topic = datastore.getTopic(url.trim(), false);
         if(topic != null) { //Exists
            response.setStatus(200);
            response.getWriter().println("false");
         } else {
            topic = datastore.getTopic(url.trim(), true);
            response.setStatus(201);
            response.getWriter().println("true");
         }
      } catch(Exception se) {
         logger.error("Problem adding topic", se);
         response.sendError(500);
      }
   }

   private final Map<String, Integer> extendLeaseValues = ImmutableMap.of(
           "hour", 3600,
           "day", 24 * 3600,
           "week", 24 * 7 * 3600,
           "month", 24 * 7 * 30 * 3600,
           "never", Integer.MAX_VALUE
   );

   private int translateExtendLease(final String extendLease) {
      Integer extendLeaseSeconds = extendLeaseValues.get(extendLease != null ? extendLease : "week");
      return extendLeaseSeconds != null ? extendLeaseSeconds : extendLeaseValues.get("week");
   }


   private void postSubscriptionEdit(final HttpServletRequest request,
                                     final String idStr,
                                     final HttpServletResponse response) throws IOException {
      if(idStr == null || idStr.trim().length() == 0) {
         postSubscriptionAdd(request, response);
         return;
      }

      String action = request.getParameter("op");
      //Expect: 'enable', disable', 'expire', 'extend'

      int extendLeaseSeconds = translateExtendLease(request.getParameter("extendLease"));

      try {
         long id = Long.parseLong(idStr);
         Subscription subscription = datastore.getSubscription(id);

         if(subscription != null) {

            Subscriber currSubscriber = datastore.getSubscriber(subscription.getEndpointId());
            if(currSubscriber == null) {
               response.sendError(500, "Data integrity violation");
               return;
            }

            String callbackUsername = Strings.nullToEmpty(request.getParameter("callbackUsername")).trim();
            String callbackPassword = Strings.nullToEmpty(request.getParameter("callbackPassword")).trim();

            Subscriber newSubscriber = null;

            if(callbackUsername.length() > 0 && callbackPassword.length() > 0) {
               AuthScheme authScheme = datastore.resolveAuthScheme("Basic");
               String authId = getBasicAuthId(callbackUsername, callbackPassword);
               newSubscriber = datastore.getSubscriber(currSubscriber.getURL(), authScheme, authId, true);
               if(newSubscriber.getId() != currSubscriber.getId()) { //Change the endpoint...
                  subscription = new Subscription.Builder(subscription, newSubscriber).create();
                  datastore.updateSubscription(subscription, false);
               }
            }

            if(action == null || action.length() == 0) {
               response.getWriter().print(subscription.getStatus().toString());
               response.setStatus(200);
            } else if(action.equals("enable")) {
               if(!subscription.isActive()) {
                  datastore.changeSubscriptionStatus(id, Subscription.Status.ACTIVE, extendLeaseSeconds);
               }
               response.setStatus(200);
               response.getWriter().print("ACTIVE");
            } else if(action.equals("disable")) {
               if(!subscription.isRemoved()) {
                  datastore.changeSubscriptionStatus(id, Subscription.Status.REMOVED, 0);
               }
               response.setStatus(200);
               response.getWriter().print("REMOVED");
            } else if(action.equals("expire")) {
               if(!subscription.isExpired()) {
                  datastore.expireSubscription(id);
               }
               response.setStatus(200);
               response.getWriter().print("EXPIRED");
            } else if(action.equals("extend")) {
               datastore.changeSubscriptionStatus(id, Subscription.Status.ACTIVE, extendLeaseSeconds);
               response.getWriter().print("ACTIVE");
               response.setStatus(200);
            } else {
               response.getWriter().print(subscription.getStatus().toString());
               response.setStatus(200);
            }
         } else {
            sendNotFound(response);
         }
      } catch(IOException ioe) {
         throw ioe;
      } catch(Exception se) {
         logger.error("Problem editing subscription", se);
         response.sendError(500);
      }
   }

   private final URIEncoder urlEncoder = new URIEncoder();

   private void postSubscriptionAdd(final HttpServletRequest request, final HttpServletResponse response) throws IOException {

      try {

         String topicURL = request.getParameter("topicURL");

         if(topicURL == null || topicURL.length() == 0) {
            response.sendError(400, "The 'topicURL' must be specified");
            return;
         }

         try {
            topicURL = urlEncoder.recode(topicURL);
         } catch(Exception e) {
            response.sendError(400, "The 'topicURL' is invalid");
            return;
         }

         String callbackURL = request.getParameter("callbackURL");

         if(callbackURL == null || callbackURL.length() == 0) {
            response.sendError(400, "The 'callbackURL' must be specified");
            return;
         }

         try {
            callbackURL = urlEncoder.recode(callbackURL);
         } catch(Exception e) {
            response.sendError(400, "The 'callbackURL' is invalid");
            return;
         }

         Subscription existingSubscription = datastore.getSubscription(topicURL, callbackURL);
         if(existingSubscription != null) { //Nope...editing..
            response.setStatus(200);
            response.getWriter().print(existingSubscription.getId());
            return;
         }

         Subscription.Status status = Subscription.Status.REMOVED;
         String statusStr = request.getParameter("status");
         if(statusStr.equals("active")) {
            status = Subscription.Status.ACTIVE;
         } else if(statusStr.equals("expired")) {
            status = Subscription.Status.EXPIRED;
         }

         String callbackHostURL = org.attribyte.api.http.Request.getHostURL(callbackURL);
         String callbackAuthScheme = request.getParameter("callbackAuthScheme");
         String authId = null;
         AuthScheme authScheme = null;

         if(callbackAuthScheme != null && callbackAuthScheme.equalsIgnoreCase("basic")) {
            String callbackUsername = Strings.nullToEmpty(request.getParameter("callbackUsername"));
            String callbackPassword = Strings.nullToEmpty(request.getParameter("callbackPassword"));
            if(callbackUsername.length() > 0 && callbackPassword.length() > 0) {
               authScheme = datastore.resolveAuthScheme("Basic");
               authId = getBasicAuthId(callbackUsername, callbackPassword);
            }
         } else {
            response.sendError(400, "Only 'Basic' auth is currently supported");
            return;
         }

         int extendLeaseSeconds = translateExtendLease(request.getParameter("extendLease"));
         String hubSecret = request.getParameter("hubSecret");

         Subscriber subscriber = datastore.getSubscriber(callbackHostURL, authScheme, authId, true); //Create, if required.

         Topic topic = datastore.getTopic(topicURL, true);
         Subscription.Builder builder = new Subscription.Builder(0L, callbackURL, topic, subscriber);
         builder.setStatus(status);
         builder.setLeaseSeconds(extendLeaseSeconds);
         builder.setSecret(hubSecret);
         Subscription updatedSubscription = datastore.updateSubscription(builder.create(), status == Subscription.Status.ACTIVE);
         response.setStatus(201);
         response.getWriter().print("ok");
      } catch(IOException ioe) {
         throw ioe;
      } catch(Exception se) {
         logger.error("Problem creating subscription", se);
         response.sendError(500);
      }
   }

   private void sendNotFound(HttpServletResponse response) throws IOException {
      response.sendError(404, "Not Found");
   }

   /**
    * Gets the endpoint basic authId.
    * @param username The username.
    * @param password The password.
    * @return The auth id.
    */
   private String getBasicAuthId(final String username, final String password) {
      return BasicAuthScheme.buildAuthHeaderValue(username, password).substring("Basic ".length()).trim();
   }

   /**
    * Gets a template instance.
    * @param name The template name.
    * @return The instance.
    */
   private ST getTemplate(final String name) {
      try {
         if(debug) templateGroup.unload();
         ST template = templateGroup.getInstanceOf(name);
         if(template != null && name.equals("main")) { //Add metadata...
            template.add("hostname", InetAddress.getLocalHost().getHostName());
            template.add("time", new Date());
         }
         return template;
      } catch(Exception e) {
         e.printStackTrace();
         return null;
      }
   }

   static final class ErrorListener implements STErrorListener {

      public void compileTimeError(STMessage msg) {
         System.out.println(msg);
      }

      public void runTimeError(STMessage msg) {
         System.out.println(msg);
      }

      public void IOError(STMessage msg) {
         System.out.println(msg);
      }

      public void internalError(STMessage msg) {
         System.out.println(msg);
      }
   }

   private final HubDatastore datastore;
   private final HubEndpoint endpoint;
   private final Logger logger;
   private final AdminAuth auth;
   private final STGroup templateGroup;
   private final boolean debug = true;
   private final int maxPerPage = 10;
   private final int pageRequestSize = maxPerPage + 1;
}