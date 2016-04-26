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

package org.attribyte.api.pubsub.impl.server;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.Set;

@SuppressWarnings("serial")
public class PingServlet extends HttpServlet {

   /**
    * Creates the servlet.
    * @param instanceName The instance name.
    * @param enumerateInterfaces Should network interface names be enumerated?
    */
   public PingServlet(final String instanceName, final boolean enumerateInterfaces) {
      this.instanceName = instanceName;
      this.enumerateInterfaces = enumerateInterfaces;
   }

   private static final String CONTENT_TYPE = "text/plain";
   private static final String CACHE_CONTROL_HEADER = "Cache-Control";
   private static final String NO_CACHE_VALUE = "must-revalidate,no-cache,no-store";
   private static final Joiner nameJoiner = Joiner.on(',').skipNulls();
   private final String instanceName;
   private final boolean enumerateInterfaces;

   @Override
   protected void doGet(HttpServletRequest request,
                        HttpServletResponse response) throws ServletException, IOException {
      response.setStatus(200);
      response.setHeader(CACHE_CONTROL_HEADER, NO_CACHE_VALUE);
      response.setContentType(CONTENT_TYPE);
      try(PrintWriter writer = response.getWriter()) {
         writer.println(nameJoiner.join(getPublicNames()));
      }
   }

   /**
    * Gets all public names (IP, hostname).
    * Always starts with "pong" and contains the instance name, if specified.
    * @return The list of names.
    */
   private Set<String> getPublicNames() {

      Set<String> names = Sets.newHashSetWithExpectedSize(8);
      names.add("pong");
      if(instanceName != null && instanceName.length() > 0) {
         names.add(instanceName);
      }

      if(enumerateInterfaces) {
         try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while(interfaces.hasMoreElements()) {
               NetworkInterface iface = interfaces.nextElement();
               if(!iface.isLoopback() && !iface.isPointToPoint() && !iface.isVirtual() && iface.isUp()) {
                  for(final InterfaceAddress interfaceAddress : iface.getInterfaceAddresses()) {
                     InetAddress addy = interfaceAddress.getAddress();
                     if(!addy.isLoopbackAddress() &&
                             !addy.isMulticastAddress() &&
                             !addy.isLinkLocalAddress() &&
                             !addy.isSiteLocalAddress() &&
                             !addy.isAnyLocalAddress()) {
                        String hostAddress = addy.getHostAddress();
                        if(!hostAddress.contains("%")) {
                           names.add(hostAddress);
                           String hostname = addy.getHostName();
                           if(hostname != null) {
                              names.add(hostname);
                           }
                        }
                     }
                  }
               }
            }
         } catch(SocketException se) {
            //Ignore
         }
      }
      return names;
   }

}