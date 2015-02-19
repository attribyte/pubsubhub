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

import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import org.attribyte.api.DatastoreException;
import org.attribyte.api.Logger;
import org.attribyte.api.http.Header;
import org.attribyte.api.http.Response;
import org.attribyte.api.http.impl.servlet.Bridge;
import org.attribyte.api.pubsub.BasicAuthFilter;
import org.attribyte.api.pubsub.HubDatastore;
import org.attribyte.api.pubsub.HubEndpoint;
import org.attribyte.api.pubsub.Notification;
import org.attribyte.api.pubsub.NotificationMetrics;
import org.attribyte.api.pubsub.Topic;
import org.attribyte.api.pubsub.impl.client.BasicAuth;
import org.attribyte.api.pubsub.impl.server.util.NotificationRecord;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings("serial")
/**
 * A servlet that immediately queues notifications for broadcast
 * to subscribers.
 */
public class BroadcastServlet extends ServletBase implements NotificationRecord.Source {


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
    * @param topicCache A topic cache.
    */
   public BroadcastServlet(final HubEndpoint endpoint, final Logger logger,
                           final List<BasicAuthFilter> filters,
                           final Cache<String, Topic> topicCache,
                           final Topic replicationTopic,
                           final int maxSavedNotifications) {
      this(endpoint, DEFAULT_MAX_BODY_BYTES, DEFAULT_AUTOCREATE_TOPICS, logger, filters, topicCache, replicationTopic, maxSavedNotifications);
   }

   /**
    * Creates a servlet with a specified maximum body size.
    * @param endpoint The hub endpoint.
    * @param maxBodyBytes The maximum size of accepted for a notification body.
    * @param logger The logger.
    * @param filters A list of filters to be applied.
    * @param topicCache A topic cache.
    */
   public BroadcastServlet(final HubEndpoint endpoint, final int maxBodyBytes,
                           final boolean autocreateTopics,
                           final Logger logger,
                           final List<BasicAuthFilter> filters,
                           final Cache<String, Topic> topicCache,
                           final Topic replicationTopic,
                           final int maxSavedNotifications) {
      this.endpoint = endpoint;
      this.datastore = endpoint.getDatastore();
      this.maxBodyBytes = maxBodyBytes;
      this.autocreateTopics = autocreateTopics;
      this.logger = logger;
      this.filters = filters != null ? ImmutableList.copyOf(filters) : ImmutableList.<BasicAuthFilter>of();
      this.topicCache = topicCache;
      this.replicationTopic = replicationTopic;
      this.maxSavedNotifications = maxSavedNotifications;
      this.recentNotifications = maxSavedNotifications > 0 ? new ArrayBlockingQueue<NotificationRecord>(maxSavedNotifications) : null;
      this.recentNotificationsSize = new AtomicInteger();
      if(recentNotifications != null) {
         this.recentNotificationsMonitor = new Thread(new Runnable() {
            @Override
            public void run() {
               while(true) {
                  try {
                     if(recentNotificationsSize.get() >= maxSavedNotifications) {
                        List<NotificationRecord> drain = Lists.newArrayListWithCapacity(maxSavedNotifications);
                        int numDrained = recentNotifications.drainTo(drain, maxSavedNotifications);
                        recentNotificationsSize.addAndGet(-1 * numDrained);
                     } else {
                        Thread.sleep(100L);
                     }
                  } catch(InterruptedException ie) {
                     return;
                  }
               }
            }
         });
         this.recentNotificationsMonitor.setName("recent-notifications-monitor");
         this.recentNotificationsMonitor.setDaemon(true);
         this.recentNotificationsMonitor.start();
      } else {
         this.recentNotificationsMonitor = null;
      }
   }

   @Override
   public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {

      long startNanos = System.nanoTime();

      byte[] broadcastContent = ByteStreams.toByteArray(request.getInputStream());

      long endNanos = System.nanoTime();

      String topicURL = request.getPathInfo();

      if(maxBodyBytes > 0 && broadcastContent.length > maxBodyBytes) {
         logNotification(request, topicURL, NOTIFICATION_TOO_LARGE.statusCode, null);
         Bridge.sendServletResponse(NOTIFICATION_TOO_LARGE, response);
         return;
      }

      Response endpointResponse;
      if(topicURL != null) {
         if(filters.size() > 0) {
            String checkHeader = request.getHeader(BasicAuth.AUTH_HEADER_NAME);
            for(BasicAuthFilter filter : filters) {
               if(filter.reject(topicURL, checkHeader)) {
                  logNotification(request, topicURL, Response.Code.UNAUTHORIZED, broadcastContent);
                  response.sendError(Response.Code.UNAUTHORIZED, "Unauthorized");
                  return;
               }
            }
         }

         try {

            Topic topic = topicCache != null ? topicCache.getIfPresent(topicURL) : null;
            if(topic == null) {
               topic = datastore.getTopic(topicURL, autocreateTopics);
               if(topicCache != null && topic != null) {
                  topicCache.put(topicURL, topic);
               }
            }

            if(topic != null) {
               NotificationMetrics globalMetrics = endpoint.getGlobalNotificationMetrics();
               NotificationMetrics metrics = endpoint.getNotificationMetrics(topic.getId());
               metrics.notificationSize.update(broadcastContent.length);
               globalMetrics.notificationSize.update(broadcastContent.length);
               long acceptTimeNanos = endNanos - startNanos;
               metrics.notifications.update(acceptTimeNanos, TimeUnit.NANOSECONDS);
               globalMetrics.notifications.update(acceptTimeNanos, TimeUnit.NANOSECONDS);
               Notification notification = new Notification(topic, null, broadcastContent); //No custom headers...
               endpoint.enqueueNotification(notification);
               if(replicationTopic != null) {
                  endpoint.enqueueNotification(new Notification(
                          replicationTopic,
                          Collections.singleton(new Header(REPLICATION_TOPIC_HEADER, topic.getURL())),
                          broadcastContent
                  ));
               }
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

      logNotification(request, topicURL, endpointResponse.statusCode, broadcastContent);
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
      if(isShutdown.compareAndSet(false, true)) {
         logger.info("Shutting down broadcast servlet...");
         if(recentNotificationsMonitor != null) {
            logger.info("Shutting down recent notifications monitor...");
            recentNotificationsMonitor.interrupt();
         }
         endpoint.shutdown();
         logger.info("Broadcast servlet shutdown.");
      }
   }

   /**
    * Invalidates all entries in internal caches.
    */
   public void invalidateCaches() {
      endpoint.invalidateCaches();
   }


   /**
    * Logs a notification if recent notifications are configured.
    */
   private void logNotification(final HttpServletRequest request,
                                final String topicURL, final int responseCode,
                                final byte[] body) {
      if(recentNotifications != null) {
         if(!recentNotifications.offer(new NotificationRecord(request, topicURL, responseCode, body))) {
            logger.warn("Recent notifications buffer is full! ");
         } else {
            recentNotificationsSize.incrementAndGet();
         }
      }
   }

   /**
    * Gets the most recently added notifications (if configured).
    * @param limit The maximum number returned.
    * @return A list of recent notifications.
    */
   public List<NotificationRecord> latestNotifications(final int limit) {
      if(recentNotifications != null) {
         List<NotificationRecord> records = Lists.newArrayListWithCapacity(maxSavedNotifications);
         records.addAll(recentNotifications);
         Collections.sort(records);
         if(records.size() >= limit) {
            return records.subList(0, limit);
         } else {
            return records;
         }
      } else {
         return Collections.emptyList();
      }
   }

   /**
    * The header sent to identify the topic when replicating all notifications
    * to the special replication topic ('X-Attribyte-Topic').
    */
   public static final String REPLICATION_TOPIC_HEADER = "X-Attribyte-Topic";

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

   /**
    * The topic cache.
    */
   private final Cache<String, Topic> topicCache;

   /**
    * A special topic to which all notifications are sent.
    */
   private final Topic replicationTopic;

   /**
    * Saves the N most recent notifications.
    */
   private final BlockingQueue<NotificationRecord> recentNotifications;

   /**
    * Monitors the recent notifications queue and periodically evicts.
    */
   private final Thread recentNotificationsMonitor;

   /**
    * Tracks the size of the notification queue.
    */
   private final AtomicInteger recentNotificationsSize;

   /**
    * The maximum number of recent notifications.
    */
   private final int maxSavedNotifications;
}
