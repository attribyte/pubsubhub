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

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.json.MetricsModule;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.attribyte.api.DatastoreException;
import org.attribyte.api.pubsub.HubEndpoint;
import org.attribyte.api.pubsub.NotificationMetrics;
import org.attribyte.api.pubsub.Topic;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.attribyte.api.pubsub.impl.server.util.ServerUtil.splitPath;

@SuppressWarnings("serial")
/**
 * A servlet that exposes global and per-topic notification metrics as JSON.
 */
class NotificationMetricsServlet extends HttpServlet {

   NotificationMetricsServlet(final HubEndpoint endpoint) {
      this.endpoint = endpoint;
   }

   /**
    * Maps a string to metrics sort.
    */
   private final ImmutableMap<String, NotificationMetrics.Sort> sortMap =
           ImmutableMap.<String, NotificationMetrics.Sort>builder()
                   .put("throughput:asc", NotificationMetrics.Sort.THROUGHPUT_ASC)
                   .put("throughput:desc", NotificationMetrics.Sort.THROUGHPUT_DESC)
                   .build();

   @Override
   public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {

      String joinString = Strings.nullToEmpty(request.getParameter("joinWith")).trim();
      if(joinString.isEmpty()) joinString = "_";
      boolean allowSlashInPrefix = true;
      if(request.getParameter("allowSlash") != null) {
         allowSlashInPrefix = request.getParameter("allowSlash").trim().equalsIgnoreCase("true");
      }

      List<String> path = splitPath(request);
      String obj = path.size() > 0 ? path.get(0) : null;
      String[] topics = request.getParameterValues("name");

      if(obj == null) {
         MetricRegistry registry = new MetricRegistry();
         registry.register("", endpoint.getGlobalNotificationMetrics());

         String json = mapper.writer().writeValueAsString(registry);
         response.setContentType("application/json");
         response.getWriter().print(json);
         response.getWriter().flush();
      } else if(obj.equals("topic")) {
         final List<NotificationMetrics> metrics;
         if(topics != null && topics.length > 0) {
            metrics = Lists.newArrayListWithExpectedSize(topics.length);
            for(String topicName : topics) {
               Topic topic = resolveTopic(topicName);
               if(topic != null) {
                  NotificationMetrics topicMetrics = endpoint.getNotificationMetrics(topic.getId());
                  if(topicMetrics != null) metrics.add(topicMetrics);
               }
            }
         } else {
            String sortStr = request.getParameter("sort");
            if(sortStr == null) sortStr = "";
            NotificationMetrics.Sort sort = sortMap.get(sortStr);
            if(sort == null) sort = NotificationMetrics.Sort.THROUGHPUT_DESC;
            String limitStr = request.getParameter("limit");
            int limit = 25;
            if(limitStr != null) {
               try {
                  limit = Integer.parseInt(limitStr);
               } catch(NumberFormatException nfe) {
                  limit = 25;
               }
            }
            metrics = endpoint.getNotificationMetrics(sort, limit);
         }

         MetricRegistry registry = new MetricRegistry();
         for(NotificationMetrics callbackMetrics : metrics) {
            String prefix = getTopicPrefix(callbackMetrics.topicId, joinString, allowSlashInPrefix);
            if(prefix != null) {
               registry.register(prefix, callbackMetrics);
            }
         }

         String json = mapper.writer().writeValueAsString(registry);
         response.setContentType("application/json");
         response.getWriter().print(json);
         response.getWriter().flush();
      } else {
         response.sendError(404);
      }
   }

   /**
    * Gets a metrics prefix for a topic.
    * @param topicId The topic id.
    * @param joinString The string used to join parts of the topic URI.
    * @param allowSlash Is '/' allowed in the prefix?
    * @return The prefix.
    */
   private String getTopicPrefix(final long topicId,
                                 final String joinString,
                                 final boolean allowSlash) {
      try {
         Topic topic = endpoint.getDatastore().getTopic(topicId);
         if(topic != null) {
            URI uri = new URI(topic.getURL());
            List<String> components = Lists.newArrayListWithCapacity(4);
            if(uri.getHost() != null) components.add(uri.getHost());
            if(uri.getPort() > 0) components.add(Integer.toString(uri.getPort()));
            String path = uri.getPath();
            if(path != null) {
               if(path.startsWith("/")) path = path.substring(1);
               if(path.endsWith("/")) path = path.substring(0, path.length() - 1);
               components.add(path);
            }
            String prefix = Joiner.on(joinString).skipNulls().join(components);
            if(!allowSlash) prefix = prefix.replace('/', '_');
            return prefix;
         } else {
            return null;
         }
      } catch(DatastoreException de) {
         log("Unable to resolve topic", de);
         return null;
      } catch(URISyntaxException use) {
         return null;
      }
   }

   /**
    * Resolves a topic from the URL.
    * @param url The URL.
    * @return The topic or <code>null</code> if not resolved.
    */
   private Topic resolveTopic(String url) {
      try {
         return endpoint.getDatastore().getTopic(url, false);
      } catch(DatastoreException de) {
         log("Problem resolving topic", de);
         return null;
      }
   }

   private final HubEndpoint endpoint;

   private final ObjectMapper mapper = new ObjectMapper().registerModule(new MetricsModule(
           TimeUnit.SECONDS,
           TimeUnit.MILLISECONDS,
           false));

}