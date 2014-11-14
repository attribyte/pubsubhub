package org.attribyte.api.pubsub.impl;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.RatioGauge;
import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.attribyte.api.http.Header;
import org.attribyte.api.pubsub.Subscriber;

import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * A cache for subscribers.
 */
public class SubscriberCache {

   /**
    * Creates the cache.
    * @param maxAgeMillis The maximum subscriber age in milliseconds.
    * @param evictionMonitorFrequencyMinutes The frequency at which a background monitor checks for expired entries.
    * If < 1, no background monitoring is performed.
    */
   SubscriberCache(final long maxAgeMillis, final int evictionMonitorFrequencyMinutes) {
      this.cache = new MapMaker().concurrencyLevel(8).initialCapacity(1024).makeMap();
      this.maxAgeMillis = maxAgeMillis;
      if(evictionMonitorFrequencyMinutes > 0) {
         this.evictionService = MoreExecutors.getExitingScheduledExecutorService(new ScheduledThreadPoolExecutor(1,
                 new ThreadFactoryBuilder().setNameFormat("subscriber-cache-monitor-%d").build()));
         this.evictionService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
               Iterator<CachedSubscriber> iter = cache.values().iterator();
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

   /**
    * A subscriber with an optional header required for authorization.
    */
   static final class CachedSubscriber {

      private CachedSubscriber(final Subscriber subscriber,
                               final Header authHeader,
                               final long maxAgeMillis) {
         this.subscriber = subscriber;
         this.authHeader = authHeader;
         this.cacheTimeMillis = System.currentTimeMillis();
         this.expireTimeMillis = this.cacheTimeMillis + maxAgeMillis;
      }

      /**
       * The expired status.
       * @return Has the cached subscriber expired?
       */
      boolean expired() {
         return System.currentTimeMillis() > expireTimeMillis;
      }

      final Subscriber subscriber;
      final Header authHeader;
      final long cacheTimeMillis;
      final long expireTimeMillis;
   }

   /**
    * Gets a previously cached subscriber, if not cached or expired.
    * @return The subscriber or <code>null</code> if not cached or expired.
    */
   CachedSubscriber getSubscriber(final long subscriberId) {
      requests.mark();
      CachedSubscriber subscriber = cache.get(subscriberId);
      if(subscriber != null) {
         if(subscriber.expired()) {
            cache.remove(subscriberId);
            return null;
         } else {
            hits.mark();
            return subscriber;
         }
      } else {
         return null;
      }
   }

   /**
    * Caches a subscriber.
    * @param subscriber The subscriber.
    * @param authHeader The auth header, if any.
    */
   void cacheSubscriber(final Subscriber subscriber, final Header authHeader) {
      cache.put(subscriber.getId(), new CachedSubscriber(subscriber, authHeader, maxAgeMillis));
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
   private final ConcurrentMap<Long, CachedSubscriber> cache;
   private final ScheduledExecutorService evictionService;

   final Gauge<Integer> cacheSizeGauge;
   final Meter requests;
   final Meter hits;
   final RatioGauge hitRatio;
}