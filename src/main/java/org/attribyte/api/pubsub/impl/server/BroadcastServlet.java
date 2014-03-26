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

import com.google.common.io.ByteStreams;
import org.attribyte.api.DatastoreException;
import org.attribyte.api.Logger;
import org.attribyte.api.http.Response;
import org.attribyte.api.http.impl.servlet.Bridge;
import org.attribyte.api.pubsub.HubDatastore;
import org.attribyte.api.pubsub.HubEndpoint;
import org.attribyte.api.pubsub.Notification;
import org.attribyte.api.pubsub.Topic;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * A servlet that immediately queues notifications for broadcast
 * to subscribers.
 */
public class BroadcastServlet extends ServletBase {

   /**
    * Creates a servlet with a maximum body size of 1MB.
    * @param endpoint The hub endpoint.
    */
   public BroadcastServlet(final HubEndpoint endpoint, final Logger logger) {
      this(endpoint,  1024 * 1000, logger);
   }

   /**
    * Creates a servlet with a specified maximum body size.
    * @param endpoint The hub endpoint.
    */
   public BroadcastServlet(final HubEndpoint endpoint, final int maxBodyBytes, final Logger logger) {
      this.endpoint = endpoint;
      this.datastore = endpoint.getDatastore();
      this.maxBodyBytes = maxBodyBytes;
      this.logger = logger;
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
         try {
            Topic topic = datastore.getTopic(topicURL, false);
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

}
