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
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.servlet.AsyncContext;
import javax.servlet.AsyncEvent;
import javax.servlet.AsyncListener;
import javax.servlet.ServletRequest;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

/**
 * This class maintains the state for a given request type specifically
 *  - total number of active requests (aka permits)
 *  - A queue of suspended requests
 *  - Performance metrics
 * 
 * It essentially implements a non-blocking semaphore such that whenever
 * a permit is not available, the request is suspended using Servlet 3 API
 * and added to the queue. It also implements an {@link AsyncListener} to
 * wake up a suspended request.
 */
final class RequestTypeHandler implements AsyncListener {
  static class OperationResult {
    private final boolean success;
    private final AsyncContext context;

    OperationResult(boolean success, AsyncContext context) {
      this.success = success;
      this.context = context;
    }

    public AsyncContext getContext() {
      return context;
    }

    public boolean isSuccess() {
      return success;
    }
  }

  // Semaphore related state
  private int permits;
  private final Queue<AsyncContext> queue;
  private final Lock lock;
  private final Condition permitsAvailable;
  // Metrics for this handler.
  private final Meter totalRequests;
  private final Counter activeRequests;
  private final Counter suspendedRequests;
  private final Counter runnableRequests;
  private final Meter rejectedRequests;
  private final Timer processingTime;

  public RequestTypeHandler(MetricRegistry metrics, String type, int permits) {
    this.permits = permits;
    this.queue = new LinkedList<>();
    this.lock = new ReentrantLock(true);
    this.permitsAvailable = lock.newCondition();
    this.totalRequests = metrics.meter(MetricRegistry.name("RequestTypeHandler", type, "requests", "total"));
    this.activeRequests = metrics.counter(MetricRegistry.name("RequestTypeHandler", type, "requests", "running"));
    this.suspendedRequests = metrics.counter(MetricRegistry.name("RequestTypeHandler", type, "requests", "suspended"));
    this.runnableRequests = metrics.counter(MetricRegistry.name("RequestTypeHandler", type, "requests", "runnable"));
    this.rejectedRequests = metrics.meter(MetricRegistry.name("RequestTypeHandler", type, "requests", "rejected"));
    this.processingTime = metrics.timer(MetricRegistry.name("RequestTypeHandler", type, "request", "processingTime"));
  }

  public void acquire() throws InterruptedException {
    lock.lock();
    try {
      while (permits <= 0) {
        permitsAvailable.await();
      }
      permits--;

    } finally {
      lock.unlock();
    }
  }

  public OperationResult tryAcquire(ServletRequest request, long timeOut, TimeUnit unit) throws InterruptedException {
    lock.lock();
    try {
      long currTimeNanos = System.nanoTime();
      long timeOutNanos = currTimeNanos + unit.toNanos(timeOut);

      while (permits <= 0 && (timeOutNanos > currTimeNanos)) {
        permitsAvailable.await((timeOutNanos - currTimeNanos), TimeUnit.NANOSECONDS);
        currTimeNanos = System.nanoTime();
      }

      if (permits <= 0) {// Failed to acquire a permit.
        AsyncContext context = request.startAsync();
        queue.add(context);
        return new OperationResult(false, context);
      } else {
        permits--;
        return new OperationResult(true, null);
      }
    } finally {
      lock.unlock();
    }
  }

  public boolean tryAcquire(long timeOut, TimeUnit unit) throws InterruptedException {
    lock.lock();
    try {
      long currTimeNanos = System.nanoTime();
      long timeOutNanos = currTimeNanos + unit.toNanos(timeOut);

      while (permits <= 0 && (timeOutNanos > currTimeNanos)) {
        permitsAvailable.await((timeOutNanos - currTimeNanos), TimeUnit.NANOSECONDS);
        currTimeNanos = System.nanoTime();
      }

      if (permits <= 0) {// Failed to acquire a permit.
        return false;
      } else {
        permits--;
        return true;
      }

    } finally {
      lock.unlock();
    }
  }

  public OperationResult release() {
    lock.lock();
    try {
      permits++;
      permitsAvailable.signal();
      return new OperationResult(true, queue.poll());
    } finally {
      lock.unlock();
    }
  }

  public AsyncContext pollWaitingQueue() {
    lock.lock();
    try {
      return (permits > 0) ? queue.poll() : null;
    } finally {
      lock.unlock();
    }
  }

  private boolean removeContext(AsyncContext context) {
    lock.lock();
    try {
      return queue.remove(context);
    } finally {
      lock.unlock();
    }
  }

  public Counter getActiveRequests() {
    return activeRequests;
  }

  public Counter getRunnableRequests() {
    return runnableRequests;
  }

  public Counter getSuspendedRequests() {
    return suspendedRequests;
  }

  public Meter getRejectedRequests() {
    return rejectedRequests;
  }

  public Meter getTotalRequests() {
    return totalRequests;
  }

  public Timer getProcessingTime() {
    return processingTime;
  }

  @Override
  public void onTimeout(AsyncEvent arg0) throws IOException {
    // Remove before it's redispatched, so it won't be
    // redispatched again at the end of the filtering (as part of release
    // operation).
    AsyncContext asyncContext = arg0.getAsyncContext();
    if (removeContext(asyncContext)) {
      asyncContext.dispatch();
    }
  }

  @Override
  public void onComplete(AsyncEvent arg0) throws IOException {
  }

  @Override
  public void onError(AsyncEvent arg0) throws IOException {
  }

  @Override
  public void onStartAsync(AsyncEvent arg0) throws IOException {
  }
}
