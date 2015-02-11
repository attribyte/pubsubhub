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

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.json.MetricsModule;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.attribyte.api.pubsub.CallbackMetrics;
import org.attribyte.api.pubsub.HostCallbackMetrics;
import org.attribyte.api.pubsub.HubEndpoint;
import org.attribyte.api.pubsub.SubscriptionCallbackMetrics;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.attribyte.api.pubsub.impl.server.util.ServerUtil.splitPath;

@SuppressWarnings("serial")
/**
 * A servlet that exposes global, per-host, and per-subscription callback metrics as JSON.
 */
class CallbackMetricsServlet extends HttpServlet {

   CallbackMetricsServlet(final HubEndpoint endpoint) {
      this.endpoint = endpoint;
   }

   /**
    * Maps a string to host metrics sort.
    */
   private final ImmutableMap<String, CallbackMetrics.Sort> sortMap =
           ImmutableMap.<String, CallbackMetrics.Sort>builder()
                   .put("throughput:asc", CallbackMetrics.Sort.THROUGHPUT_ASC)
                   .put("throughput:desc", CallbackMetrics.Sort.THROUGHPUT_DESC)
                   .put("failed:asc", CallbackMetrics.Sort.FAILURE_RATE_ASC)
                   .put("failed:desc", CallbackMetrics.Sort.FAILURE_RATE_DESC)
                   .put("abandoned:asc", CallbackMetrics.Sort.ABANDONED_RATE_ASC)
                   .put("abandoned:desc", CallbackMetrics.Sort.ABANDONED_RATE_DESC)
                   .build();

   @Override
   public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
      List<String> path = splitPath(request);
      String obj = path.size() > 0 ? path.get(0) : null;
      if(obj == null) {
         MetricRegistry registry = new MetricRegistry();
         registry.register("", endpoint.getGlobalCallbackMetrics());
         String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(registry);
         response.setContentType("application/json");
         response.getWriter().print(json);
         response.getWriter().flush();
      } else if(obj.equals("host")) {
         final List<HostCallbackMetrics> metrics;
         if(path.size() > 1) {
            HostCallbackMetrics hostMetrics = endpoint.getHostCallbackMetrics(path.get(1));
            metrics = hostMetrics != null ? Collections.singletonList(hostMetrics) : Collections.<HostCallbackMetrics>emptyList();
         } else {
            String sortStr = request.getParameter("sort");
            if(sortStr == null) sortStr = "";
            CallbackMetrics.Sort sort = sortMap.get(sortStr);
            if(sort == null) sort = CallbackMetrics.Sort.THROUGHPUT_DESC;
            String limitStr = request.getParameter("limit");
            int limit = 25;
            if(limitStr != null) {
               try {
                  limit = Integer.parseInt(limitStr);
               } catch(NumberFormatException nfe) {
                  limit = 25;
               }
            }
            metrics = endpoint.getHostCallbackMetrics(sort, limit);
         }

         MetricRegistry registry = new MetricRegistry();
         for(HostCallbackMetrics callbackMetrics : metrics) {
            registry.register(callbackMetrics.host, callbackMetrics);
         }

         String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(registry);
         response.setContentType("application/json");
         response.getWriter().print(json);
         response.getWriter().flush();
      } else if(obj.equals("subscription")) {
         if(path.size() > 1) {
            long subscriptionId = Long.parseLong(path.get(1));
            SubscriptionCallbackMetrics metrics = endpoint.getSubscriptionCallbackMetrics(subscriptionId);
            MetricRegistry registry = new MetricRegistry();
            registry.register("", metrics);
            String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(registry);
            response.setContentType("application/json");
            response.getWriter().print(json);
            response.getWriter().flush();
         } else {
            response.sendError(404);
         }
      } else {
         response.sendError(404);
      }
   }

   private final HubEndpoint endpoint;

   private final ObjectMapper mapper = new ObjectMapper().registerModule(new MetricsModule(
           TimeUnit.SECONDS,
           TimeUnit.MILLISECONDS,
           false));

}
