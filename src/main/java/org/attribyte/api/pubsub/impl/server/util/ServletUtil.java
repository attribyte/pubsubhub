package org.attribyte.api.pubsub.impl.server.util;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import javax.servlet.http.HttpServletRequest;
import java.util.Collections;
import java.util.List;

public class ServletUtil {

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
