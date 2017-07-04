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

package kafka.cluster;

import scala.collection._;

/**
 * The set of active brokers in the cluster
 */
private<kafka> class Cluster {
  ;
  private val brokers = new mutable.HashMap<Int, Broker>;
  ;
  public void  this(Iterable brokerList<Broker>) {
    this();
	  for(broker <- brokerList)
      brokers.put(broker.id, broker);
  }

  public void  getBroker Integer id): Option<Broker> = brokers.get(id);
  ;
  public void  add(Broker broker) = brokers.put(broker.id, broker);
  ;
  public void  remove Integer id) = brokers.remove(id);
  ;
  public void  size = brokers.size;
  ;
  override public void  String toString =
    "Cluster(" + brokers.values.mkString(", ") + ")"  ;
}
