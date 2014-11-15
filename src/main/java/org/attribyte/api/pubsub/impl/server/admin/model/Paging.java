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

package org.attribyte.api.pubsub.impl.server.admin.model;

/**
 * Paging information (starts at <code>1</code>).
 */
public class Paging {

   /**
    * Empty paging.
    */
   public static final Paging EMPTY = new Paging(1, 0, false);


   public Paging(final int currPage, final int perPage, final boolean hasMorePages) {
      this.currPage = currPage > 0 ? currPage : 1;
      this.perPage = perPage;
      this.hasMorePages = hasMorePages;
   }

   /**
    * Gets the start index.
    * @return The start index.
    */
   public int getStart() {
      return (currPage - 1) * perPage;
   }

   /**
    * Is there a previous page.
    * @return Is there a previous page?
    */
   public boolean getHasPrev() {
      return currPage > 1;
   }

   /**
    * Gets the previous page number.
    * @return The previous page number.
    */
   public int getPrev() {
      return currPage - 1;
   }

   /**
    * Gets the current page number.
    * @return The current page number.
    */
   public int getCurr() {
      return currPage;
   }

   /**
    * Gets the number per page.
    * @return The number per page.
    */
   public int getPerPage() {
      return perPage;
   }

   /**
    * Are there more pages?
    * @return Are there more pages?
    */
   public boolean getHasMore() {
      return hasMorePages;
   }

   /**
    * Gets the next page, or <code>0</code> if none.
    * @return The next page.
    */
   public int getNext() {
      return hasMorePages ? currPage + 1 : 0;
   }

   /**
    * Determine if this is the first page.
    * @return is this the first page.
    */
   public boolean getIsFirst() {
      return currPage < 2;
   }

   /**
    * Determine if this is the only page.
    * @return Is this the only page?
    */
   public boolean getIsOnlyPage() {
      return !hasMorePages && currPage == 1;
   }

   private final int currPage;
   private final int perPage;
   private final boolean hasMorePages;
}