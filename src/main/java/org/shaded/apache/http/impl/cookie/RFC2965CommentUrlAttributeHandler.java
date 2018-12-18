/*
 * ====================================================================
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 *
 */

package org.shaded.apache.http.impl.cookie;

import org.apache.http.annotation.Contract;
import org.apache.http.annotation.ThreadingBehavior;
import org.shaded.apache.http.cookie.ClientCookie;
import org.shaded.apache.http.cookie.CommonCookieAttributeHandler;
import org.shaded.apache.http.cookie.Cookie;
import org.shaded.apache.http.cookie.CookieOrigin;
import org.shaded.apache.http.cookie.MalformedCookieException;
import org.shaded.apache.http.cookie.SetCookie;
import org.shaded.apache.http.cookie.SetCookie2;

/**
 * {@code "CommentURL"} cookie attribute handler for RFC 2965 cookie spec.
 *
 * @since 4.0
 */
@Contract(threading = ThreadingBehavior.IMMUTABLE)
public class RFC2965CommentUrlAttributeHandler implements CommonCookieAttributeHandler {

      public RFC2965CommentUrlAttributeHandler() {
          super();
      }

      @Override
      public void parse(final SetCookie cookie, final String commenturl)
              throws MalformedCookieException {
          if (cookie instanceof SetCookie2) {
              final SetCookie2 cookie2 = (SetCookie2) cookie;
              cookie2.setCommentURL(commenturl);
          }
      }

      @Override
      public void validate(final Cookie cookie, final CookieOrigin origin)
              throws MalformedCookieException {
      }

      @Override
      public boolean match(final Cookie cookie, final CookieOrigin origin) {
          return true;
      }

    @Override
    public String getAttributeName() {
        return ClientCookie.COMMENTURL_ATTR;
    }

}
