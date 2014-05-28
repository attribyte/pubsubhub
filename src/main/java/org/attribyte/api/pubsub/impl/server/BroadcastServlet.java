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

package org.attribyte.api.pubsub.impl.server;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import org.attribyte.api.DatastoreException;
import org.attribyte.api.Logger;
import org.attribyte.api.http.Request;
import org.attribyte.api.http.Response;
import org.attribyte.api.http.impl.servlet.Bridge;
import org.attribyte.api.pubsub.BasicAuthFilter;
import org.attribyte.api.pubsub.HubDatastore;
import org.attribyte.api.pubsub.HubEndpoint;
import org.attribyte.api.pubsub.Notification;
import org.attribyte.api.pubsub.Topic;
import org.attribyte.api.pubsub.URLFilter;
import org.attribyte.api.pubsub.impl.client.BasicAuth;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * A servlet that immediately queues notifications for broadcast
 * to subscribers.
 */
public class BroadcastServlet extends ServletBase {

   /**
    * The default maximum body size (1MB).
    */
   public static final int DEFAULT_MAX_BODY_BYTES = 1024 * 1000;


   /**
    * The default for topic auto-create (false).
    */
   public static final boolean DEFAULT_AUTOCREATE_TOPICS = false;

   /**
    * Creates a servlet with a maximum body size of 1MB and topics that must exist
    * before notifications are accepted.
    * @param endpoint The hub endpoint.
    * @param logger A logger.
    * @param filters A list of filters to be applied.
    */
   public BroadcastServlet(final HubEndpoint endpoint, final Logger logger,
                           final List<BasicAuthFilter> filters) {
      this(endpoint, DEFAULT_MAX_BODY_BYTES, DEFAULT_AUTOCREATE_TOPICS, logger, filters);
   }

   /**
    * Creates a servlet with a specified maximum body size.
    * @param endpoint The hub endpoint.
    * @param maxBodyBytes The maximum size of accepted for a notification body.
    * @param logger The logger.
    * @param filters A ist of filters to be applied.
    */
   public BroadcastServlet(final HubEndpoint endpoint, final int maxBodyBytes,
                           final boolean autocreateTopics,
                           final Logger logger,
                           final List<BasicAuthFilter> filters) {
      this.endpoint = endpoint;
      this.datastore = endpoint.getDatastore();
      this.maxBodyBytes = maxBodyBytes;
      this.autocreateTopics = autocreateTopics;
      this.logger = logger;
      this.filters = filters != null ? ImmutableList.<BasicAuthFilter>copyOf(filters) : ImmutableList.<BasicAuthFilter>of();
   }

   @Override
   public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {

      byte[] broadcastContent = ByteStreams.toByteArray(request.getInputStream());
      if(maxBodyBytes > 0 && broadcastContent.length > maxBodyBytes) {
         Bridge.sendServletResponse(NOTIFICATION_TOO_LARGE, response);
         return;
      }

      String topicURL = request.getPathInfo();
      Response endpointResponse;
      if(topicURL != null) {
         if(filters.size() > 0) {
            String checkHeader = request.getHeader(BasicAuth.AUTH_HEADER_NAME);
            for(BasicAuthFilter filter : filters) {
               if(filter.reject(topicURL, checkHeader)) {
                  response.sendError(Response.Code.UNAUTHORIZED, "Unauthorized");
                  return;
               }
            }
         }

         try {
            Topic topic = datastore.getTopic(topicURL, autocreateTopics);
            if(topic != null) {
               Notification notification = new Notification(topic, null, broadcastContent); //No custom headers...
               endpoint.enqueueNotification(notification);
               endpointResponse = ACCEPTED_RESPONSE;
            } else {
               endpointResponse = UNKNOWN_TOPIC_RESPONSE;
            }
         } catch(DatastoreException de) {
            logger.error("Problem selecting topic", de);
            endpointResponse = INTERNAL_ERROR_RESPONSE;
         }
      } else {
         endpointResponse = NO_TOPIC_RESPONSE;
      }

      Bridge.sendServletResponse(endpointResponse, response);
   }

   @Override
   public void destroy() {
      shutdown();
   }

   /**
    * Shutdown the servlet.
    */
   public void shutdown() {
      logger.info("Shutting down broadcast servlet...");
      if(isShutdown.compareAndSet(false, true)) {
         endpoint.shutdown();
      }
      logger.info("Broadcast servlet shutdown.");
   }

   /**
    * The hub endpoint.
    */
   private final HubEndpoint endpoint;

   /**
    * The hub datastore.
    */
   private final HubDatastore datastore;

   /**
    * The maximum accepted body size.
    */
   private final int maxBodyBytes;

   /**
    * The logger.
    */
   private final Logger logger;

   /**
    * Ensure shutdown happens only once.
    */
   private AtomicBoolean isShutdown = new AtomicBoolean(false);

   /**
    * A list of basic auth filters.
    */
   private final List<BasicAuthFilter> filters;


   /**
    * Should unknown topics be automatically created?
    */
   private final boolean autocreateTopics;

}
