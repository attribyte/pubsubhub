package org.attribyte.api.pubsub.impl.server.util;

/**
 * Any resource that can be invalidated, like a cache.
 */
public interface Invalidatable {

   /**
    * Invalidate it.
    */
   public void invalidate();
}
