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

package org.attribyte.api.pubsub.impl.server.util;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import javax.servlet.http.HttpServletRequest;
import java.util.Collections;
import java.util.List;

public class ServerUtil {

   /**
    * The system property that points to the installation directory.
    */
   public static final String PUBSUB_INSTALL_DIR_SYSPROP = "pubsub.install.dir";

   /**
    * Gets the system install directory (always ends with '/').
    * @return The directory.
    */
   public static String systemInstallDir() {
      String systemInstallDir = System.getProperty(PUBSUB_INSTALL_DIR_SYSPROP, "").trim();
      if(systemInstallDir.length() > 0 && !systemInstallDir.endsWith("/")) {
         systemInstallDir = systemInstallDir + "/";
      }
      return systemInstallDir;
   }

   /**
    * Splits the path into a list of components.
    * @param request The request.
    * @return The path.
    */
   public static List<String> splitPath(final HttpServletRequest request) {
      String pathInfo = request.getPathInfo();
      if(pathInfo == null || pathInfo.length() == 0 || pathInfo.equals("/")) {
         return Collections.emptyList();
      } else {
         return Lists.newArrayList(pathSplitter.split(pathInfo));
      }
   }

   private static final Splitter pathSplitter = Splitter.on('/').omitEmptyStrings().trimResults();

}
