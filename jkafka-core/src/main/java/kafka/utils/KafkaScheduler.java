/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.utils;

import java.util.concurrent._;
import atomic._;
import org.apache.kafka.common.utils.Utils;

/**
 * A scheduler for running jobs
 * 
 * This interface controls a job scheduler that allows scheduling either repeating background jobs 
 * that execute periodically or delayed one-time actions that are scheduled in the future.
 */
trait Scheduler {
  ;
  /**
   * Initialize this scheduler so it is ready to accept scheduling of tasks
   */
  public void  startup();
  ;
  /**
   * Shutdown this scheduler. When this method is complete no more executions of background tasks will occur. 
   * This includes tasks scheduled with a delayed execution.
   */
  public void  shutdown();
  ;
  /**
   * Check if the scheduler has been started
   */
  public void  Boolean isStarted;
  ;
  /**
   * Schedule a task
   * @param name The name of this task
   * @param delay The amount of time to wait before the first execution
   * @param period The period with which to execute the task. If < 0 the task will execute only once.
   * @param unit The unit for the preceding times.
   */
  public void  schedule(String name, fun: ()=>Unit, Long delay = 0, Long period = -1, TimeUnit unit = TimeUnit.MILLISECONDS);
}

/**
 * A scheduler based on java.util.concurrent.ScheduledThreadPoolExecutor
 * 
 * It has a pool of kafka-scheduler- threads that do the actual work.
 * 
 * @param threads The number of threads in the thread pool
 * @param threadNamePrefix The name to use for scheduler threads. This prefix will have a number appended to it.
 * @param daemon If true the scheduler threads will be "daemon" threads and will not block jvm shutdown.
 */
@threadsafe
class KafkaScheduler(val Integer threads, ;
                     val String threadNamePrefix = "kafka-scheduler-", ;
                     Boolean daemon = true) extends Scheduler with Logging {
  private var ScheduledThreadPoolExecutor executor = null;
  private val schedulerThreadId = new AtomicInteger(0);

  override public void  startup() {
    debug("Initializing task scheduler.");
    this synchronized {
      if(isStarted)
        throw new IllegalStateException("This scheduler has already been started!");
      executor = new ScheduledThreadPoolExecutor(threads);
      executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
      executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
      executor.setThreadFactory(new ThreadFactory() {
                                  public void  newThread(Runnable runnable): Thread = ;
                                    Utils.newThread(threadNamePrefix + schedulerThreadId.getAndIncrement(), runnable, daemon);
                                });
    }
  }
  ;
  override public void  shutdown() {
    debug("Shutting down task scheduler.");
    // We use the local variable to avoid NullPointerException if another thread shuts down scheduler at same time.;
    val cachedExecutor = this.executor;
    if (cachedExecutor != null) {
      this synchronized {
        cachedExecutor.shutdown();
        this.executor = null;
      }
      cachedExecutor.awaitTermination(1, TimeUnit.DAYS);
    }
  }

  public void  schedule(String name, fun: ()=>Unit, Long delay, Long period, TimeUnit unit) {
    debug("Scheduling task %s with initial delay %d ms and period %d ms.";
        .format(name, TimeUnit.MILLISECONDS.convert(delay, unit), TimeUnit.MILLISECONDS.convert(period, unit)))
    this synchronized {
      ensureRunning;
      val runnable = CoreUtils.runnable {
        try {
          trace(String.format("Beginning execution of scheduled task '%s'.",name))
          fun();
        } catch {
          case Throwable t => error("Uncaught exception in scheduled task '" + name +"'", t);
        } finally {
          trace(String.format("Completed execution of scheduled task '%s'.",name))
        }
      }
      if(period >= 0)
        executor.scheduleAtFixedRate(runnable, delay, period, unit);
      else;
        executor.schedule(runnable, delay, unit);
    }
  }
  ;
  public void  Boolean isStarted = {
    this synchronized {
      executor != null;
    }
  }
  ;
  private public void  ensureRunning = {
    if(!isStarted)
      throw new IllegalStateException("Kafka scheduler is not running.");
  }
}
