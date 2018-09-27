/*
 * Copyright 2014 Attribyte, LLC
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

import org.attribyte.api.Logger;
import org.attribyte.api.pubsub.HubEndpoint;
import org.attribyte.api.pubsub.impl.server.util.NotificationRecord;
import org.attribyte.api.pubsub.impl.server.util.Invalidatable;
import org.attribyte.api.pubsub.impl.server.util.SubscriptionEvent;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Enables the admin console.
 */
public class AdminConsole {

   /**
    * Creates the console.
    * @param rootContext The root context for servlets.
    * @param assetDirectory The path to static assets.
    * @param endpoint The hub endpoint.
    * @param auth Admin-specific auth.
    * @param templateDirectory The template directory.
    * @param logger A logger.
    */
   public AdminConsole(final ServletContextHandler rootContext,
                       String assetDirectory,
                       final HubEndpoint endpoint,
                       final AdminAuth auth,
                       final String templateDirectory,
                       final Logger logger) {
      this.endpoint = endpoint;
      this.auth = auth;
      this.templateDirectory = templateDirectory;
      this.logger = logger;

      if(!assetDirectory.endsWith("/")) {
         assetDirectory = assetDirectory + "/";
      }

      rootContext.addAliasCheck(new ContextHandler.ApproveAliases());
      rootContext.setInitParameter("org.eclipse.jetty.servlet.Default.resourceBase", assetDirectory);
      rootContext.setInitParameter("org.eclipse.jetty.servlet.Default.acceptRanges", "false");
      rootContext.setInitParameter("org.eclipse.jetty.servlet.Default.dirAllowed", "false");
      rootContext.setInitParameter("org.eclipse.jetty.servlet.Default.welcomeServlets", "true");
      rootContext.setInitParameter("org.eclipse.jetty.servlet.Default.redirectWelcome", "false");
      rootContext.setInitParameter("org.eclipse.jetty.servlet.Default.aliases", "true");
      rootContext.setInitParameter("org.eclipse.jetty.servlet.Default.gzip", "true");
   }


   /**
    * Initialize the servlets.
    * @param rootContext The root context.
    * @param adminPath The path to the admin servlet.
    * @param allowedAssetPaths A list of paths (relative to the base directory)
    * from which static assets will be returned (<code>/css, /js, ...</code>).
    * @param invalidatables A collection of caches, etc that may be invalidated on-demand.
    * @param subscriptionEventSource A source for subscription events.
    * @param notificationSource A source for recent notifications.
    */
   public void initServlets(final ServletContextHandler rootContext,
                            String adminPath,
                            final List<String> allowedAssetPaths,
                            final Collection<Invalidatable> invalidatables,
                            final SubscriptionEvent.Source subscriptionEventSource,
                            final NotificationRecord.Source notificationSource) {
      if(servletInit.compareAndSet(false, true)) {
         DefaultServlet defaultServlet = new DefaultServlet();
         for(String path : allowedAssetPaths) {
            logger.info("AdminConsole: Adding allowed asset path, '" + path + "'");
            rootContext.addServlet(new ServletHolder(defaultServlet), path);
         }
         if(!adminPath.endsWith("/")) {
            adminPath = adminPath + "/";
         }

         logger.info("AdminConsole: Enabled on path, '" + adminPath + "'");
         rootContext.addServlet(new ServletHolder(new AdminServlet(endpoint, invalidatables,
                 subscriptionEventSource, notificationSource,
                 auth, templateDirectory, logger)), adminPath + "*");
      }
   }

   private final HubEndpoint endpoint;
   private final AdminAuth auth;
   private final String templateDirectory;
   private final Logger logger;
   private final AtomicBoolean servletInit = new AtomicBoolean(false);
}