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

package org.attribyte.api.pubsub;

import org.attribyte.api.http.Request;

import java.util.Properties;

/**
 * A filter that rejects URLs that contain a fragment (#).
 */
public class FragmentRejectFilter implements URLFilter {

   @Override
   public boolean reject(String url, Request request) {
      return url.indexOf('#') > 0;
   }

   public void init(final Properties props) {}

   public boolean shutdown(final int waitTimeSeconds) { return true; }
}