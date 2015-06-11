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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.servlet.AsyncContext;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;

import org.apache.solr.scheduling.RequestTypeHandler.OperationResult;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Counter;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;

/**
 * Request scheduling filter
 * <p>
 * This filter limits the total number of active requests for a given request
 * type to a configured maximum value. If more requests are received, they are
 * suspended and placed a queue. It also ensures to resume the suspended
 * requests whenever a thread is available for processing and total number of
 * active requests for a given type are below the configured maximum value.
 * <p>
 * An application can provide a custom {@link RequestSchedulingPolicy} via a
 * Java system property named 'requestPolicyClassName'. This policy
 * implementation needs to provide the necessary configuration parameters
 * required by this filter viz.
 * - A list of request types accepted by the application. (Note - this list must be
 *   exhaustive i.e. it should cover *all* requests processed by the application).
 * - Total number of requests which can be processed concurrently for every request
 *   type
 * - Time duration (in ms) the scheduler waits for acquiring a permit (to process
 *   the request) before suspending it.
 * - Time duration (in ms) after which the suspended request is woken up again. This
 *   can be disabled by configuring this parameter to -1.
 */
public class RequestSchedulerFilter implements Filter {
  public static final String REQUEST_SCHEDULING_ENABLED_PARAM = "requestSchedulingEnabled";
  public static final String REQUEST_POLICY_CLASS_PARAM = "requestPolicyClassName";
  public static final String REQUEST_SCHEDULING_WAIT_PARAM = "waitTimeMs";
  public static final String REQUEST_SCHEDULING_SUSPEND_PARAM = "suspendTimeMs";
  public static final String ENABLE_JMX_REPORTING_PARAM = "enableJmxReporting";
  public static final String ENABLE_CONSOLE_REPORTING_PARAM = "enableConsoleReporting";

  private final String suspended_attr_name = "RequestSchedulerFilter@" + Integer.toHexString(hashCode()) + ".SUSPENDED";
  private final String resumd_attr_name = "RequestSchedulerFilter@" + Integer.toHexString(hashCode()) + ".RESUMED";
  private long waitMs;
  private long suspendMs;
  private boolean enableScheduling = true;
  private RequestSchedulingPolicy policy;
  private final Map<String, RequestTypeHandler> config = new HashMap<>();
  // Used only when request scheduling is disabled.
  private Meter globalTotalRequests;
  private Counter globalActiveRequests;
  private Timer globalProcessingTime;

  private final MetricRegistry metrics = new MetricRegistry();
  private final JmxReporter reporter = JmxReporter.forRegistry(metrics).build();
  private final ConsoleReporter consoleReporter = ConsoleReporter.forRegistry(metrics).convertRatesTo(TimeUnit.SECONDS)
      .convertDurationsTo(TimeUnit.MILLISECONDS).build();

  @Override
  public void init(FilterConfig arg0) throws ServletException {
    enableScheduling = Boolean.getBoolean(REQUEST_SCHEDULING_ENABLED_PARAM);
    if (enableScheduling) {
      String policyClassName = System.getProperty(REQUEST_POLICY_CLASS_PARAM);
      try {
        Class<?> klass = Thread.currentThread().getContextClassLoader().loadClass(policyClassName);
        policy = (RequestSchedulingPolicy) klass.newInstance();
      } catch (ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
        throw new ServletException(ex);
      }
      this.waitMs = policy.getWaitTimeMs();
      this.suspendMs = policy.getSuspendTimeMs();

      assert (policy.getRequestTypes() != null);

      for (String type : policy.getRequestTypes()) {
        RequestTypeHandler handler = new RequestTypeHandler(metrics, type, policy.getMaxActiveRequests(type));
        config.put(type, handler);
      }
    } else {
      this.globalTotalRequests = metrics.meter(MetricRegistry.name("global", "requests", "total"));
      this.globalActiveRequests = metrics.counter(MetricRegistry.name("global", "requests", "running"));
      this.globalProcessingTime = metrics.timer(MetricRegistry.name("global", "request", "processingTime"));
    }

    if (Boolean.getBoolean(ENABLE_JMX_REPORTING_PARAM)) {
      reporter.start();
    }
    if (Boolean.getBoolean(ENABLE_CONSOLE_REPORTING_PARAM)) {
      consoleReporter.start(1, TimeUnit.SECONDS);
    }
  }

  @Override
  public void destroy() {
    if (Boolean.getBoolean(ENABLE_JMX_REPORTING_PARAM)) {
      reporter.stop();
    }
    if (Boolean.getBoolean(ENABLE_CONSOLE_REPORTING_PARAM)) {
      consoleReporter.stop();
    }
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
    if (enableScheduling) {
      RequestTypeHandler handler = config.get(policy.getRequestType(request));
      Context ctx = handler.getProcessingTime().time();
      boolean accepted = false;
      try {
        Boolean suspended = (Boolean) request.getAttribute(suspended_attr_name);
        if (suspended == null) {
          OperationResult result = handler.tryAcquire(request, waitMs, TimeUnit.MILLISECONDS);
          accepted = result.isSuccess();
          if (accepted) {
            request.setAttribute(suspended_attr_name, Boolean.FALSE);
            // System.out.printf("Accepted %s \n", request);
          } else {
            request.setAttribute(suspended_attr_name, Boolean.TRUE);
            if (suspendMs > 0) {
              result.getContext().setTimeout(suspendMs);
              result.getContext().addListener(handler);
            }
            handler.getSuspendedRequests().inc();
            // System.out.printf("Suspended %s \n", request);
            return;
          }
        } else {
          if (suspended) {
            request.setAttribute(suspended_attr_name, Boolean.FALSE);
            Boolean resumed = (Boolean) request.getAttribute(resumd_attr_name);
            if (resumed == Boolean.TRUE) {
              handler.acquire();
              accepted = true;
              handler.getRunnableRequests().dec();
              // System.out.printf("Resumed %s \n", request);
            } else {
              // Timeout! try 1 more time.
              accepted = handler.tryAcquire(waitMs, TimeUnit.MILLISECONDS);
              // System.out.printf("Timeout %s \n", request);
            }
          } else {
            // Pass through resume of previously accepted request.
            handler.acquire();
            accepted = true;
            // System.out.printf("Passthrough %s \n", request);
          }
        }

        if (accepted) {
          handler.getActiveRequests().inc();
          chain.doFilter(request, response);
        } else {
          handler.getRejectedRequests().mark();
          // System.out.printf("Rejected %s \n", request);
          ((HttpServletResponse) response).sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        }
      } catch (InterruptedException e) {
        ((HttpServletResponse) response).sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
      } finally {
        // Record the request processing time.
        ctx.stop();

        if (accepted) {
          // Update the total number of requests processed.
          handler.getTotalRequests().mark();

          // Update the number of active requests.
          handler.getActiveRequests().dec();

          // Attempt to resume a previously suspended request.
          OperationResult result = handler.release();
          AsyncContext asyncContext = result.getContext();
          if (asyncContext == null) {// There is no outstanding request for this
                                     // request type.
            // Try to to steal a request from some other request type.
            for (Map.Entry<String, RequestTypeHandler> entry : config.entrySet()) {
              asyncContext = entry.getValue().pollWaitingQueue();
              if (asyncContext != null) {
                handler = entry.getValue();
                break;
              }
            }
          }

          if (asyncContext != null) {// There is an outstanding request
                                     // available
                                     // for scheduling.
            // Update the metric (handler refers to the appropriate request type
            // handler).
            handler.getSuspendedRequests().dec();
            handler.getRunnableRequests().inc();
            ServletRequest candidate = asyncContext.getRequest();
            Boolean suspended = (Boolean) candidate.getAttribute(suspended_attr_name);
            if (suspended == Boolean.TRUE) {
              candidate.setAttribute(resumd_attr_name, Boolean.TRUE);
              asyncContext.dispatch();
            }
          }
        }
      }
    } else {// The request scheduling is disabled.
      // Just forward the request down the servlet chain and record the
      // statistics.
      Context ctx = globalProcessingTime.time();
      try {
        globalActiveRequests.inc();
        chain.doFilter(request, response);
      } finally {
        // Update the total number of requests processed.
        globalTotalRequests.mark();
        // Update the number of active requests.
        globalActiveRequests.dec();
        // Record the request processing time.
        ctx.stop();
      }
    }
  }
}
