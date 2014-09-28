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

public class PingServlet extends HttpServlet {


   public PingServlet(final String instanceName) {
      this.instanceName = instanceName;
   }


   private static final String CONTENT_TYPE = "text/plain";
   private static final String CACHE_CONTROL_HEADER = "Cache-Control";
   private static final String NO_CACHE_VALUE = "must-revalidate,no-cache,no-store";
   private static final Joiner nameJoiner = Joiner.on(',').skipNulls();
   private final String instanceName;

   @Override
   protected void doGet(HttpServletRequest request,
                        HttpServletResponse response) throws ServletException, IOException {
      response.setStatus(200);
      response.setHeader(CACHE_CONTROL_HEADER, NO_CACHE_VALUE);
      response.setContentType(CONTENT_TYPE);
      PrintWriter writer = response.getWriter();
      try {
         writer.println(nameJoiner.join(getPublicNames()));
      } finally {
         writer.close();
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

      try {
         Enumeration interfaces = NetworkInterface.getNetworkInterfaces();
         while(interfaces.hasMoreElements()) {
            NetworkInterface iface = (NetworkInterface)interfaces.nextElement();
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

      return names;
   }

}