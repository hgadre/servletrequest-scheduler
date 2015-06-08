package org.apache.solr.scheduling;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import javax.servlet.ServletRequest;

/**
 * This interface defines a policy which can be implemented by an application
 * integrating with {@link RequestSchedulerFilter} to provide application
 * specific request type identification and resource requirements.
 */
public interface RequestSchedulingPolicy {

  /**
   * This method returns the request type for a given servlet request.
   *
   * @param request The servlet request for which the type needs to be identified.
   * @return The request type for a given request.
   */
  String getRequestType(ServletRequest request);

  /**
   * This method returns an array of configured request types. Every request
   * received by this web application must be able to map to one of these types.
   *
   * @return an array of configured request types.
   */
  String[] getRequestTypes();

  /**
   * This method returns the maximum number of active requests configured for a
   * given request type.
   */
  public int getMaxActiveRequests(String requestType);

  /**
   * This method returns the waiting time (in ms) the request scheduler should
   * wait before suspending the request.
   */
  public long getWaitTimeMs();

  /**
   * This method returns the suspension time (in ms) after which the suspended
   * request is woken up. This feature can be disabled by returning -1.
   */
  public long getSuspendTimeMs();
}
