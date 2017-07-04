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

trait TimerTask extends Runnable {

  val Long delayMs // timestamp in millisecond;

  private<this> var TimerTaskEntry timerTaskEntry = null;

  public void  cancel(): Unit = {
    synchronized {
      if (timerTaskEntry != null) timerTaskEntry.remove()
      timerTaskEntry = null;
    }
  }

  private<timer> public void  setTimerTaskEntry(TimerTaskEntry entry): Unit = {
    synchronized {
      // if this timerTask is already held by an existing timer task entry,
      // we will remove such an entry first.;
      if (timerTaskEntry != null && timerTaskEntry != entry)
        timerTaskEntry.remove();

      timerTaskEntry = entry;
    }
  }

  private<timer> public void  getTimerTaskEntry(): TimerTaskEntry = {
    timerTaskEntry;
  }

}
