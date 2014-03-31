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

import org.attribyte.api.Logger;
import org.attribyte.api.http.Request;
import org.attribyte.api.http.Response;
import org.attribyte.api.http.ResponseBuilder;
import org.attribyte.api.http.impl.servlet.Bridge;
import org.attribyte.api.pubsub.HubEndpoint;
import org.attribyte.api.pubsub.HubMode;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * A servlet interface for <code>HubEndpoint</code>.
 */
public class HubServlet extends HttpServlet {

   /**
    * Creates a servlet with a maximum body size of 1MB.
    * @param endpoint The hub endpoint.
    * @param logger The logger.
    */
   public HubServlet(final HubEndpoint endpoint, final Logger logger) {
      this(endpoint, 1024 * 1000, logger);
   }

   /**
    * Creates a servlet with a specified maximum body size.
    * @param endpoint The hub endpoint.
    * @param maxBodyBytes The maximum body size in bytes.
    * @param logger The logger.
    */
   public HubServlet(final HubEndpoint endpoint, final int maxBodyBytes, final Logger logger) {
      this.endpoint = endpoint;
      this.MAX_BODY_BYTES = maxBodyBytes;
      this.logger = logger;
   }

   @Override
   public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {

      Request endpointRequest = Bridge.fromServletRequest(request, MAX_BODY_BYTES);
      final Response endpointResponse;

      String hubMode = request.getParameter("hub.mode");
      if(hubMode == null) {
         endpointResponse = new ResponseBuilder(Response.Code.BAD_REQUEST, "A 'hub.mode' must be specified").create();
      } else {
         switch(HubMode.fromString(hubMode)) {
            case SUBSCRIBE:
            case UNSUBSCRIBE:
               endpointResponse = endpoint.subscriptionRequest(endpointRequest);
               break;
            default:
               endpointResponse = new ResponseBuilder(Response.Code.BAD_REQUEST, "The 'hub.mode' is unsupported").create();
         }
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
   protected final HubEndpoint endpoint;

   /**
    * The maximum accepted body size.
    */
   protected final int MAX_BODY_BYTES;

   /**
    * The logger.
    */
   protected final Logger logger;

   /**
    * Ensure shutdown happens only once.
    */
   private final AtomicBoolean isShutdown = new AtomicBoolean(false);
}
