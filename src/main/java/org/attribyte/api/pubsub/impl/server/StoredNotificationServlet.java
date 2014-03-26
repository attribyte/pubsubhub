/*
 * Copyright 2010 Attribyte, LLC 
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

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import org.attribyte.api.DatastoreException;
import org.attribyte.api.Logger;
import org.attribyte.api.http.Response;
import org.attribyte.api.http.impl.servlet.Bridge;
import org.attribyte.api.pubsub.HubDatastore;
import org.attribyte.api.pubsub.HubEndpoint;
import org.attribyte.api.pubsub.Notification;
import org.attribyte.api.pubsub.Topic;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * A servlet that first stores all <code>Notifications</code> such that,
 * on failure or shutdown, none are lost (TM).
 */
public class StoredNotificationServlet extends ServletBase {

   /**
    * Creates a servlet with a maximum body size of 1MB that stores
    * notifications with the topic URL as key and the content converted
    * to a UTF_8 string.
    * @param endpoint The hub endpoint.
    * @param jedisPool The Jedis client pool.
    * @param logger The logger.
    */
   public StoredNotificationServlet(final Collection<String> topics,
                                    final HubEndpoint endpoint,
                                    final JedisPool jedisPool,
                                    final Logger logger) throws DatastoreException {
      this(topics, endpoint,  1024 * 1000, jedisPool, logger);
   }

   /**
    * Creates a servlet with a specified maximum body size.
    * @param endpoint The hub endpoint.
    */
   public StoredNotificationServlet(
           final Collection<String> topics,
           final HubEndpoint endpoint,
           final int maxBodyBytes,
           final JedisPool jedisPool,
           final Logger logger) throws DatastoreException {
      assert(maxBodyBytes > 0);
      assert(endpoint != null);
      assert(jedisPool != null);
      assert(logger != null);
      assert(topics != null && topics.size() > 0);
      this.endpoint = endpoint;
      this.datastore = endpoint.getDatastore();
      this.MAX_BODY_BYTES = maxBodyBytes;
      this.jedisPool = jedisPool;
      this.logger = logger;
      this.processors = Lists.newArrayListWithCapacity(topics.size());
      this.topicProcessorThreads = Lists.newArrayListWithCapacity(topics.size());
      int count = 0;
      for(String topicURL : topics) {
         Topic topic = datastore.getTopic(topicURL, true); //Create the topic if it doesn't exist.
         logger.info("Starting topic processor for '"+topicURL+"'...");
         TopicProcessor processor = new TopicProcessor(topic, 3, 10); //TODO 3 seconds, 10 items...
         Thread processorThread = new Thread(processor);
         processorThread.setName("TopicProcessor-"+count);
         this.processors.add(processor);
         this.topicProcessorThreads.add(processorThread);
         count++;
      }

      logger.info("Starting "+topicProcessorThreads.size()+" processor threads...");

      for(Thread thread : topicProcessorThreads) {
         thread.start();
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
    * The Jedis client.
    */
   private final JedisPool jedisPool;
   
   /**
    * The maximum accepted body size.
    */
   private final int MAX_BODY_BYTES;

   /**
    * The logger.
    */
   private final Logger logger;

   /**
    * A cache of topic vs hash code.
    */
   private final Map<String, Topic> topicCache = Maps.newConcurrentMap();

   @Override
   public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {

      byte[] broadcastContent = ByteStreams.toByteArray(request.getInputStream());
      String topicURL = request.getPathInfo();
      Response endpointResponse;
      if(topicURL != null) {
         try {
            Topic topic = datastore.getTopic(topicURL, false);
            if(topic != null) {
               saveNotification(topic, broadcastContent);
               endpointResponse = ACCEPTED_RESPONSE;
            } else {
               endpointResponse = UNKNOWN_TOPIC_RESPONSE;
            }
         } catch(DatastoreException de) {
            endpointResponse = INTERNAL_ERROR_RESPONSE;
         }
      } else {
         endpointResponse = NO_TOPIC_RESPONSE;
      }

      Bridge.sendServletResponse(endpointResponse, response);
   }


   /**
    * Saves a notification by left-pushing it to a Redis list with
    * the topic URL as the key.
    * @param topic The topic.
    * @param broadcastContent The content that will eventually be broadcast to subscribers.
    * @throws DatastoreException on write error.
    */
   private void saveNotification(final Topic topic, byte[] broadcastContent) throws DatastoreException {
      Jedis jedis = jedisPool.getResource();
      try {
         long queueSize = jedis.lpush(topic.getURL().getBytes(Charsets.UTF_8), broadcastContent);
      } catch(JedisException je) {
         jedisPool.returnBrokenResource(jedis);
         jedis = null;
         throw new DatastoreException("Problem writing to Redis", je);
      } finally {
         if(jedis != null) {
            jedisPool.returnResource(jedis);
         }
      }
   }

   private class TopicProcessor implements Runnable {

      TopicProcessor(final Topic topic, final int timeoutSeconds, final int maxPopped) {
         this.topic = topic;
         this.topicKey = topic.getURL().getBytes(Charsets.UTF_8);
         this.timeoutSeconds = timeoutSeconds;
         this.maxPopped = maxPopped;
      }

      volatile boolean running = true;

      public void run() {
         while(running) {
            Jedis jedis = jedisPool.getResource();
            try {
               List<byte[]> values = jedis.blpop(timeoutSeconds, topicKey);
               if(values != null && values.size() > 1) {
                  endpoint.enqueueNotification(new Notification(topic, null, values.get(1))); //Second is value...
                  int count = 0; //If we unblock, let's try to get a bunch of values...
                  byte[] val;
                  do {
                     val = jedis.lpop(topicKey);
                     endpoint.enqueueNotification(new Notification(topic, null, val));
                  } while(val != null && count++ < maxPopped);
               }
            } catch(JedisException je) {
               jedisPool.returnBrokenResource(jedis);
               jedis = null;
               logger.error("Problem reading from Redis", je);
               sleepOnError();
            } finally {
               if(jedis != null) {
                  jedisPool.returnResource(jedis);
               }
            }
         }
      }

      /**
       * Sleep on error and, if interrupted, stop running.
       */
      private void sleepOnError() {
         try {
            Thread.sleep(ERROR_SLEEP_MILLIS);
         } catch(InterruptedException ie) {
            running = false;
            Thread.currentThread().interrupt();
         }
      }

      private final Topic topic;
      private final byte[] topicKey;
      private final int timeoutSeconds;
      private final int maxPopped;
      private static final long ERROR_SLEEP_MILLIS = 1000;

   }

   private final Collection<TopicProcessor> processors;
   private final Collection<Thread> topicProcessorThreads;
   private final AtomicBoolean isShutdown = new AtomicBoolean(false);

   /**
    * Shutdown the servlet.
    */
   public void shutdown() {
      if(isShutdown.compareAndSet(false, true)) {
         for(Thread thread : topicProcessorThreads) {
            thread.interrupt();
         }
         for(TopicProcessor processor : processors) {
            processor.running = false;
         }
      }
   }

   @Override
   public void destroy() {
      shutdown();
   }
}
