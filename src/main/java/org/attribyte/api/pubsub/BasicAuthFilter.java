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

import com.google.common.collect.Lists;
import org.attribyte.api.http.Request;
import org.attribyte.api.http.impl.BasicAuthScheme;
import org.attribyte.util.InitUtil;
import org.attribyte.util.StringUtil;

import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * A filter that rejects URLs that contain a fragment (#).
 */
public class BasicAuthFilter implements URLFilter {

   /**
    * An instance of basic auth scheme with empty 'realm'.
    */
   private static final BasicAuthScheme BASIC_AUTH_SCHEME = new BasicAuthScheme();

   /**
    * Holds auth associated with a pattern.
    */
   private static class PatternAuth {

      PatternAuth(final Pattern pattern, final String username, final String password) {
         this.pattern = pattern;
         this.username = username;
         this.password = password;
      }

      final Pattern pattern;
      final String username;
      final String password;
   }

   @Override
   public boolean reject(String url, Request request) {
      if(patterns != null) {
         for(PatternAuth patternAuth : patterns) {
            if(patternAuth.pattern.matcher(url).matches()) {
               try {
                  //Reject when there's a response...it is the unauthorized response.
                  return BASIC_AUTH_SCHEME.authenticate(request, patternAuth.username, patternAuth.password) != null;
               } catch(GeneralSecurityException se) {
                  return true; //Really...this should never happen...
               }
            }
         }
         return false;
      } else {
         return false;
      }
   }

   public void init(final Properties props) {
      Map<String, Properties> filterPropertyMap = new InitUtil("basicauth", props, false).split();
      List<String> keys = Lists.newArrayList(filterPropertyMap.keySet());
      Collections.sort(keys);
      patterns = Lists.newArrayListWithCapacity(keys.size());
      for(String filterKey : keys) {
         Properties filterProps = filterPropertyMap.get(filterKey);
         String pattern = filterProps.getProperty("pattern");
         String username = filterProps.getProperty("username");
         String password = filterProps.getProperty("password");
         if(StringUtil.hasContent(pattern) &&
                 StringUtil.hasContent(username) &&
                 StringUtil.hasContent(password)) {
            patterns.add(new PatternAuth(Pattern.compile(pattern), username, password));
         }
      }
   }

   /**
    * The list of patterns.
    */
   private List<PatternAuth> patterns;

   public boolean shutdown(final int waitTimeSeconds) { return true; }
}