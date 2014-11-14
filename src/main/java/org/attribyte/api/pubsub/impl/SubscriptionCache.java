package org.attribyte.api.pubsub.impl;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.RatioGauge;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.attribyte.api.pubsub.Subscription;
import org.attribyte.api.pubsub.Topic;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * A cache of subscriptions for topics.
 */
public class SubscriptionCache {

   /**
    * Creates the cache.
    * @param maxAgeMillis The maximum subscription age in milliseconds.
    * @param evictionMonitorFrequencyMinutes The frequency at which a background monitor checks for expired entries.
    * If < 1, no background monitoring is performed.
    */
   SubscriptionCache(final long maxAgeMillis, final int evictionMonitorFrequencyMinutes) {
      this.cache = new MapMaker().concurrencyLevel(8).initialCapacity(1024).makeMap();
      this.maxAgeMillis = maxAgeMillis;
      if(evictionMonitorFrequencyMinutes > 0) {
         this.evictionService = MoreExecutors.getExitingScheduledExecutorService(new ScheduledThreadPoolExecutor(1,
                 new ThreadFactoryBuilder().setNameFormat("subscription-cache-monitor-%d").build()));
         this.evictionService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
               Iterator<CachedSubscriptions> iter = cache.values().iterator();
               while(iter.hasNext()) {
                  if(iter.next().expired()) {
                     iter.remove();
                  }
               }
            }
         }, evictionMonitorFrequencyMinutes, evictionMonitorFrequencyMinutes, TimeUnit.MINUTES);
      } else {
         this.evictionService = null;
      }

      this.cacheSizeGauge = new Gauge<Integer>() {
         @Override
         public Integer getValue() {
            return cache.size();
         }
      };
      this.requests = new Meter();
      this.hits = new Meter();
      this.hitRatio = new RatioGauge() {
         @Override
         protected Ratio getRatio() {
            return Ratio.of(hits.getOneMinuteRate(), requests.getOneMinuteRate());
         }
      };
   }

   private static final class CachedSubscriptions {

      private CachedSubscriptions(final long topicId,
                                  final ImmutableList<Subscription> subscriptions,
                                  final long maxAgeMillis) {
         this.topicId = topicId;
         this.subscriptions = subscriptions;
         this.cacheTimeMillis = System.currentTimeMillis();
         this.expireTimeMillis = this.cacheTimeMillis + maxAgeMillis;
      }

      boolean expired() {
         return System.currentTimeMillis() > expireTimeMillis;
      }

      final long topicId;
      final ImmutableList<Subscription> subscriptions;
      final long cacheTimeMillis;
      final long expireTimeMillis;
   }

   /**
    * Gets previously cached subscriptions if they have not expired.
    * @param topic The topic.
    * @return The subscriptions or <code>null</code> if not cached or expired.
    */
   List<Subscription> getSubscriptions(final Topic topic) {
      requests.mark();
      CachedSubscriptions subscriptions = cache.get(topic.getId());
      if(subscriptions != null) {
         if(subscriptions.expired()) {
            cache.remove(topic.getId());
            return null;
         } else {
            hits.mark();
            return subscriptions.subscriptions;
         }
      } else {
         return null;
      }
   }

   /**
    * Puts subscriptions into the cache.
    * @param topic The topic.
    * @param subscriptions The subscriptions.
    */
   void cacheSubscriptions(final Topic topic, final ImmutableList<Subscription> subscriptions) {
      cache.put(topic.getId(), new CachedSubscriptions(topic.getId(), subscriptions, maxAgeMillis));
   }

   /**
    * Clears the cache.
    */
   void clear() {
      cache.clear();
   }

   /**
    * Shutdown the eviction service.
    */
   void shutdown() {
      evictionService.shutdown();
   }

   private final long maxAgeMillis;
   private final ConcurrentMap<Long, CachedSubscriptions> cache;
   private final ScheduledExecutorService evictionService;

   final Gauge<Integer> cacheSizeGauge;
   final Meter requests;
   final Meter hits;
   final RatioGauge hitRatio;
}