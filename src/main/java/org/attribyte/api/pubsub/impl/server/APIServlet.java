/*
 * Copyright 2015 Attribyte, LLC
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

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import org.attribyte.api.DatastoreException;
import org.attribyte.api.Logger;
import org.attribyte.api.http.Request;
import org.attribyte.api.pubsub.HubDatastore;
import org.attribyte.api.pubsub.HubEndpoint;
import org.attribyte.api.pubsub.Topic;
import org.attribyte.api.pubsub.URLFilter;
import org.attribyte.api.pubsub.impl.server.util.ServerUtil;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.attribyte.api.http.impl.servlet.Bridge.fromServletRequest;
import static org.attribyte.api.pubsub.impl.server.util.ServerUtil.JSON_CONTENT_TYPE;
import static org.attribyte.api.pubsub.impl.server.util.ServerUtil.splitPath;
import static org.attribyte.api.http.impl.servlet.Bridge.sendServletResponse;

@SuppressWarnings("serial")
/**
 * A servlet that exposes a JSON API.
 */
public class APIServlet extends ServletBase {

   /**
    * The default maximum body size (1MB).
    */
   public static final int DEFAULT_MAX_BODY_BYTES = 1024 * 1000;

   /**
    * Creates a servlet with a specified maximum body size.
    * @param endpoint The hub endpoint.
    * @param logger The logger.
    * @param topicFilters A list of filters that accept/reject topics.
    */
   public APIServlet(final HubEndpoint endpoint,
                     final Logger logger,
                     final List<URLFilter> topicFilters) {
      this.endpoint = endpoint;
      this.datastore = endpoint.getDatastore();
      this.logger = logger;
      this.topicFilters = topicFilters != null ? ImmutableList.copyOf(topicFilters) : ImmutableList.<URLFilter>of();
   }

   @Override
   public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
      List<String> path = splitPath(request);
      if(path.size() == 0) {
         sendServletResponse(NO_OPERATION_RESPONSE, response);
         return;
      }

      Iterator<String> pathIter = path.iterator();
      String op = pathIter.next();
      switch(op) {
         case "topics":
            doTopicsGet(request, response);
            break;
         default:
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "The operation, '" + op + "' is not supported");
            break;
      }
   }

   @Override
   public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
      List<String> path = splitPath(request);
      if(path.size() == 0) {
         sendServletResponse(NO_OPERATION_RESPONSE, response);
         return;
      }

      Iterator<String> pathIter = path.iterator();
      String op = pathIter.next();
      switch(op) {
         case "topics":
            if(!pathIter.hasNext()) {
               sendServletResponse(NO_TOPIC_RESPONSE, response);
               return;
            } else {
               doTopicPost(request, pathIter.next(), response);
            }
            break;
         default:
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "The operation, '" + op + "' is not supported");
            break;
      }
   }

   /**
    * The default page size.
    */
   public static final int DEFAULT_PAGE_SIZE = 100;

   private void doTopicsGet(HttpServletRequest request,
                            HttpServletResponse response) throws IOException, ServletException {

      int pageSize = ServerUtil.getParameter(request, "pageSize", DEFAULT_PAGE_SIZE);
      if(pageSize < 1) pageSize = DEFAULT_PAGE_SIZE;

      try {
         List<Topic> topics = datastore.getTopics(0, pageSize);
         ObjectNode responseNode = JsonNodeFactory.instance.objectNode();
         ArrayNode topicsNode = responseNode.putArray("topics");
         for(Topic topic : topics) {
            ObjectNode topicObj = topicsNode.addObject();
            topicObj.put("name", topic.getURL());
            topicObj.put("id", topic.getId());
         }
         sendJSONResponse(response, responseNode);
      } catch(DatastoreException de) {
         logger.error("Problem selecting topics", de);
         sendServletResponse(INTERNAL_ERROR_RESPONSE, response);
      }
   }

   private void doTopicPost(HttpServletRequest request, final String topic,
                            HttpServletResponse response) throws IOException, ServletException {
      try {
         Topic endpointTopic = datastore.getTopic(topic, true);
         Request checkRequest = fromServletRequest(request, DEFAULT_MAX_BODY_BYTES);
         for(URLFilter filter : topicFilters) {
            URLFilter.Result res = filter.apply(topic, checkRequest);
            if(res.rejected) {
               if(res.rejectReason != null) {
                  response.sendError(res.rejectCode, res.rejectReason);
               } else {
                  response.sendError(res.rejectCode);
               }
               return;
            }
         }

         ObjectNode responseNode = JsonNodeFactory.instance.objectNode();
         responseNode.put("name", endpointTopic.getURL());
         responseNode.put("id", endpointTopic.getId());
         sendJSONResponse(response, responseNode);
      } catch(DatastoreException de) {
         logger.error("Problem creating topic", de);
         sendServletResponse(INTERNAL_ERROR_RESPONSE, response);
      }
   }

   /**
    * Sends a JSON response.
    * @param response The HTTP response.
    * @param responseNode The JSON node to send.
    * @throws IOException on write error.
    */
   private void sendJSONResponse(HttpServletResponse response, ObjectNode responseNode) throws IOException {
      response.setStatus(HttpServletResponse.SC_OK);
      response.setContentType(JSON_CONTENT_TYPE);
      response.getOutputStream().write(responseNode.toString().getBytes(Charsets.UTF_8));
   }

   @Override
   public void destroy() {
      shutdown();
   }

   /**
    * Shutdown the servlet.
    */
   public void shutdown() {
      if(isShutdown.compareAndSet(false, true)) {
         logger.info("Shutting down api servlet...");
         logger.info("API servlet shutdown.");
      }
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
    * The logger.
    */
   private final Logger logger;

   /**
    * Ensure shutdown happens only once.
    */
   private AtomicBoolean isShutdown = new AtomicBoolean(false);

   /**
    * A list of filters for topics.
    */
   private final List<URLFilter> topicFilters;
}
