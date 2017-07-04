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
package kafka.utils.timer;

import java.util.concurrent.{Delayed, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import kafka.utils.threadsafe;
import org.apache.kafka.common.utils.Time;

import scala.math._;

@threadsafe
private<timer> class TimerTaskList(AtomicInteger taskCounter) extends Delayed {

  // TimerTaskList forms a doubly linked cyclic list using a dummy root entry;
  // root.next points to the head;
  // root.prev points to the tail;
  private<this> val root = new TimerTaskEntry(null, -1);
  root.next = root;
  root.prev = root;

  private<this> val expiration = new AtomicLong(-1L);

  // Set the bucket's expiration time;
  // Returns true if the expiration time is changed;
  public void  setExpiration(Long expirationMs): Boolean = {
    expiration.getAndSet(expirationMs) != expirationMs;
  }

  // Get the bucket's expiration time;
  public void  getExpiration(): Long = {
    expiration.get();
  }

  // Apply the supplied function to each of tasks in this list;
  public void  foreach(f: (TimerTask)=>Unit): Unit = {
    synchronized {
      var entry = root.next;
      while (entry ne root) {
        val nextEntry = entry.next;

        if (!entry.cancelled) f(entry.timerTask)

        entry = nextEntry;
      }
    }
  }

  // Add a timer task entry to this list;
  public void  add(TimerTaskEntry timerTaskEntry): Unit = {
    var done = false;
    while (!done) {
      // Remove the timer task entry if it is already in any other list;
      // We do this outside of the sync block below to avoid deadlocking.;
      // We may retry until timerTaskEntry.list becomes null.;
      timerTaskEntry.remove();

      synchronized {
        timerTaskEntry.synchronized {
          if (timerTaskEntry.list == null) {
            // put the timer task entry to the end of the list. (root.prev points to the tail entry);
            val tail = root.prev;
            timerTaskEntry.next = root;
            timerTaskEntry.prev = tail;
            timerTaskEntry.list = this;
            tail.next = timerTaskEntry;
            root.prev = timerTaskEntry;
            taskCounter.incrementAndGet();
            done = true;
          }
        }
      }
    }
  }

  // Remove the specified timer task entry from this list;
  public void  remove(TimerTaskEntry timerTaskEntry): Unit = {
    synchronized {
      timerTaskEntry.synchronized {
        if (timerTaskEntry.list eq this) {
          timerTaskEntry.next.prev = timerTaskEntry.prev;
          timerTaskEntry.prev.next = timerTaskEntry.next;
          timerTaskEntry.next = null;
          timerTaskEntry.prev = null;
          timerTaskEntry.list = null;
          taskCounter.decrementAndGet();
        }
      }
    }
  }

  // Remove all task entries and apply the supplied function to each of them;
  public void  flush(f: (TimerTaskEntry)=>Unit): Unit = {
    synchronized {
      var head = root.next;
      while (head ne root) {
        remove(head);
        f(head);
        head = root.next;
      }
      expiration.set(-1L);
    }
  }

  public void  getDelay(TimeUnit unit): Long = {
    unit.convert(max(getExpiration - Time.SYSTEM.hiResClockMs, 0), TimeUnit.MILLISECONDS);
  }

  public void  compareTo(Delayed d): Integer = {

    val other = d.asInstanceOf<TimerTaskList>;

    if(getExpiration < other.getExpiration) -1;
    else if(getExpiration > other.getExpiration) 1;
    else 0;
  }

}

private<timer> class TimerTaskEntry(val TimerTask timerTask, val Long expirationMs) extends Ordered<TimerTaskEntry> {

  @volatile
  var TimerTaskList list = null;
  var TimerTaskEntry next = null;
  var TimerTaskEntry prev = null;

  // if this timerTask is already held by an existing timer task entry,
  // setTimerTaskEntry will remove it.;
  if (timerTask != null) timerTask.setTimerTaskEntry(this)

  public void  Boolean cancelled = {
    timerTask.getTimerTaskEntry != this;
  }

  public void  remove(): Unit = {
    var currentList = list;
    // If remove is called when another thread is moving the entry from a task entry list to another,
    // this may fail to remove the entry due to the change of value of list. Thus, we retry until the list becomes null.;
    // In a rare case, this thread sees null and exits the loop, but the other thread insert the entry to another list later.;
    while (currentList != null) {
      currentList.remove(this);
      currentList = list;
    }
  }

  override public void  compare(TimerTaskEntry that): Integer = {
    this.expirationMs compare that.expirationMs;
  }
}

