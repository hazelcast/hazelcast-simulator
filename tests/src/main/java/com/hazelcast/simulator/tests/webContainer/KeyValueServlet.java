/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.simulator.tests.webContainer;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/*
* A servlet to run inside a web container.  This servlet accepts http Put and Get requests,  and implements a simple
* key value rest service.  a http put request with a uri of "key/123/789"  will put key 123 and value 789 into the
* session, for this user,  also the value put "789" is written back to the user.
* A http get request with a uri of "key/123"  will get the value for key 123 from this clients session.
* */
public class KeyValueServlet extends HttpServlet {

    private Pattern getKeyPattern = Pattern.compile("key/.*");
    private Pattern putKeyPattern = Pattern.compile("key/.*/.*");

    public KeyValueServlet(){
        System.out.println("deployed "+this.getClass());
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        HttpSession session = req.getSession();

        Matcher keyMatched = getKeyPattern.matcher(req.getRequestURI());
        if (keyMatched.find()) {

            String[] split = keyMatched.group().split("/");
            String key = split[1];

            Object value = session.getAttribute(key);

            if(value==null){
                resp.getWriter().write("key="+key+" Not found in session "+session.getId());
            }
            else{
                resp.getWriter().write(value.toString());
            }
        }
    }

    @Override
    protected void doPut(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        HttpSession session = req.getSession();

        Matcher keyMatched = putKeyPattern.matcher(req.getRequestURI());
        if (keyMatched.find()) {

            String[] split = keyMatched.group().split("/");
            String key = split[1];
            String value = split[2];

            session.setAttribute(key, value);
            resp.getWriter().write(value.toString());
        }
    }
}
