package org.attribyte.api.pubsub.impl.server.admin;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.attribyte.api.DatastoreException;
import org.attribyte.api.Logger;
import org.attribyte.api.pubsub.Endpoint;
import org.attribyte.api.pubsub.HubDatastore;
import org.attribyte.api.pubsub.Subscription;
import org.attribyte.api.pubsub.Topic;
import org.attribyte.api.pubsub.impl.server.admin.model.DisplaySubscribedHost;
import org.attribyte.api.pubsub.impl.server.admin.model.DisplayTopic;
import org.attribyte.util.URIEncoder;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STErrorListener;
import org.stringtemplate.v4.STGroup;
import org.stringtemplate.v4.STGroupDir;
import org.stringtemplate.v4.misc.STMessage;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class AdminServlet extends HttpServlet {

   public AdminServlet(final HubDatastore datastore, final AdminAuth auth,
                       final String templateDirectory,
                       final Logger logger) {
      this.datastore = datastore;
      this.auth = auth;
      this.templateGroup = new STGroupDir(templateDirectory, '$', '$');
      this.templateGroup.setListener(new ErrorListener());
      this.logger = logger;
   }

   public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
      if(!auth.authIsValid(request, response)) return;
   }

   public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
      if(!auth.authIsValid(request, response)) return;
      List<String> path = splitPath(request);
      String obj = path.size() > 0 ? path.get(0) : null;

      if(obj == null || obj.equals("topics")) {
         renderTopics(request, response);
      } else if(obj.equals("subscribers")) {
         renderSubscribers(request, response);
      } else if(obj.equals("topic")) {
         if(path.size() > 1) {
            renderTopicSubscriptions(path.get(1), request, response);
         } else {
            response.sendError(404, "Not Found");
         }
      } else if(obj.equals("host")) {
         if(path.size() > 1) {
            renderHostSubscriptions(path.get(1), request, response);
         } else {
            response.sendError(404, "Not Found");
         }
      } else if(obj.equals("subscriptions")) {
         renderAllSubscriptions(request, response);
      } else {
         response.sendError(404, "Not Found");
      }
   }

   /**
    * Splits the path into a list of components.
    * @param request The request.
    * @return The path.
    */
   private List<String> splitPath(final HttpServletRequest request) {
      String pathInfo = request.getPathInfo();
      if(pathInfo == null || pathInfo.length() == 0 || pathInfo.equals("/")) {
         return Collections.emptyList();
      } else {
         return Lists.newArrayList(pathSplitter.split(pathInfo));
      }
   }

   private void renderSubscribers(final HttpServletRequest request,
                                  final HttpServletResponse response) throws IOException {

      ST mainTemplate = getTemplate("main");
      ST subscriberTemplate = getTemplate("subscribers");

      try {
         List<DisplaySubscribedHost> subscribers = Lists.newArrayListWithExpectedSize(25);
         List<String> endpoints = datastore.getSubscribedHosts(0, 50);
         for(String host : endpoints) {
            subscribers.add(new DisplaySubscribedHost(host, datastore.countActiveHostSubscriptions(host)));
         }
         subscriberTemplate.add("subscribers", subscribers);
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
                             final HttpServletResponse response) throws IOException {

      ST mainTemplate = getTemplate("main");
      ST subscriberTemplate = getTemplate("topics");

      try {
         List<DisplayTopic> displayTopics = Lists.newArrayListWithExpectedSize(25);
         List<Topic> topics = datastore.getTopics(0, 50);
         for(Topic topic : topics) {
            displayTopics.add(new DisplayTopic(topic, datastore.countActiveSubscriptions(topic.getId())));
         }
         subscriberTemplate.add("topics", displayTopics);
         mainTemplate.add("content", subscriberTemplate.render());
         response.setContentType("text/html");
         response.getWriter().print(mainTemplate.render());
         response.getWriter().flush();
      } catch(Exception se) {
         se.printStackTrace();
         response.sendError(500, "Datastore error");
      }
   }

   private Set<Subscription.Status> getSubscriptionStatus(HttpServletRequest request) {
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
                                         final HttpServletResponse response) throws IOException {

      ST mainTemplate = getTemplate("main");
      ST subscriptionsTemplate = getTemplate("topic_subscriptions");

      try {
         long topicId = Long.parseLong(topicIdStr);
         Topic topic = datastore.getTopic(topicId);
         if(topic == null) {
            response.sendError(404, "Not Found");
            return;
         }

         List<Subscription> subscriptions = datastore.getTopicSubscriptions(topic, getSubscriptionStatus(request), 0, 50);
         subscriptionsTemplate.add("subscriptions", subscriptions);
         subscriptionsTemplate.add("topic", new DisplayTopic(topic, 0));
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
                                        final HttpServletResponse response) throws IOException {

      ST mainTemplate = getTemplate("main");
      ST subscriptionsTemplate = getTemplate("host_subscriptions");

      try {

         List<Subscription> subscriptions = datastore.getHostSubscriptions(host, getSubscriptionStatus(request), 0, 50);
         subscriptionsTemplate.add("subscriptions", subscriptions);
         subscriptionsTemplate.add("host", host);
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
                                       final HttpServletResponse response) throws IOException {

      ST mainTemplate = getTemplate("main");
      ST subscriptionsTemplate = getTemplate("all_subscriptions");

      try {

         List<Subscription> subscriptions = datastore.getSubscriptions(getSubscriptionStatus(request), 0, 50);
         subscriptionsTemplate.add("subscriptions", subscriptions);
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

   /**
    * Gets a template instance.
    * @param name The template name.
    * @return The instance.
    */
   private ST getTemplate(final String name) {
      try {
         if(debug) templateGroup.unload();
         return templateGroup.getInstanceOf(name);
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
   private final Logger logger;
   private final AdminAuth auth;
   private final STGroup templateGroup;
   private final Splitter pathSplitter = Splitter.on('/').omitEmptyStrings().trimResults();
   private final boolean debug = true;
}