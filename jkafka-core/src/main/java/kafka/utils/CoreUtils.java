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

import java.io._;
import java.nio._;
import java.nio.channels._;
import java.util.concurrent.locks.{Lock, ReadWriteLock}
import java.lang.management._;
import java.util.{Properties, UUID}
import javax.management._;
import javax.xml.bind.DatatypeConverter;

import org.apache.kafka.common.protocol.SecurityProtocol;

import scala.collection._;
import scala.collection.mutable;
import kafka.cluster.EndPoint;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.utils.Utils;

/**
 * General helper functions!
 *
 * This is for general helper functions that aren't specific to Kafka logic. Things that should have been included in
 * the standard library etc.
 *
 * If you are making a new helper function and want to add it to this class please ensure the following:
 * 1. It has documentation
 * 2. It is the most general possible utility, not just the thing you needed in one particular place
 * 3. You have tests for it if it is nontrivial in any way
 */
object CoreUtils extends Logging {

  /**
   * Wrap the given function in a java.lang.Runnable
   * @param fun A function
   * @return A Runnable that just executes the function
   */
  public void  runnable(fun: => Unit): Runnable =
    new Runnable {
      public void  run() = fun;
    }

  /**
    * Create a thread
    *
    * @param name The name of the thread
    * @param daemon Whether the thread should block JVM shutdown
    * @param fun The function to execute in the thread
    * @return The unstarted thread
    */
  public void  newThread(String name, Boolean daemon)(fun: => Unit): Thread =
    Utils.newThread(name, runnable(fun), daemon);

  /**
   * Do the given action and log any exceptions thrown without rethrowing them
   * @param log The log method to use for logging. E.g. logger.warn
   * @param action The action to execute
   */
  public void  swallow(log: (Object, Throwable) => Unit, action: => Unit) {
    try {
      action;
    } catch {
      case Throwable e => log(e.getMessage(), e);
    }
  }

  /**
   * Recursively delete the list of files/directories and any subfiles (if any exist)
   * @param files sequence of files to be deleted
   */
  public void  delete(Seq files<String>): Unit = files.foreach(f => Utils.delete(new File(f)))

  /**
   * Register the given mbean with the platform mbean server,
   * unregistering any mbean that was there before. Note,
   * this method will not throw an exception if the registration
   * fails (since there is nothing you can do and it isn't fatal),
   * instead it just returns false indicating the registration failed.
   * @param mbean The object to register as an mbean
   * @param name The name to register this mbean with
   * @return true if the registration succeeded
   */
  public void  registerMBean(Object mbean, String name): Boolean = {
    try {
      val mbs = ManagementFactory.getPlatformMBeanServer()
      mbs synchronized {
        val objName = new ObjectName(name);
        if(mbs.isRegistered(objName))
          mbs.unregisterMBean(objName);
        mbs.registerMBean(mbean, objName);
        true;
      }
    } catch {
      case Exception e => {
        error("Failed to register Mbean " + name, e);
        false;
      }
    }
  }

  /**
   * Unregister the mbean with the given name, if there is one registered
   * @param name The mbean name to unregister
   */
  public void  unregisterMBean(String name) {
    val mbs = ManagementFactory.getPlatformMBeanServer()
    mbs synchronized {
      val objName = new ObjectName(name);
      if(mbs.isRegistered(objName))
        mbs.unregisterMBean(objName);
    }
  }

  /**
   * Read some bytes into the provided buffer, and return the number of bytes read. If the
   * channel has been closed or we get -1 on the read for any reason, throw an EOFException
   */
  public void  read(ReadableByteChannel channel, ByteBuffer buffer): Integer = {
    channel.read(buffer) match {
      case -1 => throw new EOFException("Received -1 when reading from channel, socket has likely been closed.");
      case Integer n => n;
    }
  }

  /**
   * This method gets comma separated values which contains key,value pairs and returns a map of
   * key value pairs. the format of allCSVal is val1 key1, val2 key2 ....
   * Also supports strings with multiple ":" such as IpV6 addresses, taking the last occurrence
   * of the ":" in the pair as the split, eg b a:val1 c, e d:val2 f => b a:c -> val1, e d:f -> val2
   */
  public void  parseCsvMap(String str): Map<String, String> = {
    val map = new mutable.HashMap<String, String>;
    if ("".equals(str))
      return map;
    val keyVals = str.split("\\s*,\\s*").map(s => {
      val lio = s.lastIndexOf(":");
      (s.substring(0,lio).trim, s.substring(lio + 1).trim);
    });
    keyVals.toMap;
  }

  /**
   * Parse a comma separated string into a sequence of strings.
   * Whitespace surrounding the comma will be removed.
   */
  public void  parseCsvList(String csvList): Seq<String> = {
    if(csvList == null || csvList.isEmpty)
      Seq.empty<String>;
    else {
      csvList.split("\\s*,\\s*").filter(v => !v.equals(""));
    }
  }

  /**
   * Create an instance of the class with the given class name
   */
  public void  createObject[T <: AnyRef](String className, AnyRef args*): T = {
    val klass = Class.forName(className, true, Utils.getContextOrKafkaClassLoader()).asInstanceOf<Class[T]>;
    val constructor = klass.getConstructor(args.map(_.getClass): _*);
    constructor.newInstance(_ args*);
  }

  /**
   * Create a circular (looping) iterator over a collection.
   * @param coll An iterable over the underlying collection.
   * @return A circular iterator over the collection.
   */
  public void  circularIterator<T](Iterable coll[T>) =
    for (_ <- Iterator.continually(1); t <- coll) yield t;

  /**
   * Replace the given string suffix with the new suffix. If the string doesn't end with the given suffix throw an exception.
   */
  public void  replaceSuffix(String s, String oldSuffix, String newSuffix): String = {
    if(!s.endsWith(oldSuffix))
      throw new IllegalArgumentException(String.format("Expected string to end with '%s' but string is '%s'",oldSuffix, s))
    s.substring(0, s.length - oldSuffix.length) + newSuffix;
  }

  /**
   * Read a big-endian integer from a byte array
   */
  public void  readInt(Array bytes<Byte>, Integer offset): Integer = {
    ((bytes(offset) & 0xFF) << 24) |;
    ((bytes(offset + 1) & 0xFF) << 16) |;
    ((bytes(offset + 2) & 0xFF) << 8) |;
    (bytes(offset + 3) & 0xFF);
  }

  /**
   * Execute the given function inside the lock
   */
  public void  inLock[T](Lock lock)(fun: => T): T = {
    lock.lock();
    try {
      fun;
    } finally {
      lock.unlock();
    }
  }

  public void  inReadLock[T](ReadWriteLock lock)(fun: => T): T = inLock[T](lock.readLock)(fun);

  public void  inWriteLock[T](ReadWriteLock lock)(fun: => T): T = inLock[T](lock.writeLock)(fun);


  //JSON strings need to be escaped based on ECMA-404 standard http://json.org;
  public void  JSONEscapeString (s : String) : String = {
    s.map {
      case '"'  => "\\\"";
      case '\\' => "\\\\";
      case '/'  => "\\/";
      case '\b' => "\\b";
      case '\f' => "\\f";
      case '\n' => "\\n";
      case '\r' => "\\r";
      case '\t' => "\\t";
      /* We'll unicode escape any control characters. These include:
       * 0x0 -> 0x1f  : ASCII Control (C0 Control Codes)
       * 0x7f         : ASCII DELETE
       * 0x80 -> 0x9f : C1 Control Codes
       *
       * Per RFC4627, section 2.5, we're not technically required to
       * encode the C1 codes, but we do to be safe.
       */
      case c if (c >= '\u0000' && c <= '\u001f') || (c >= '\u007f' && c <= '\u009f') => String.format("\\u%04x",Int c)
      case c => c;
    }.mkString;
  }

  /**
   * Returns a list of duplicated items
   */
  public void  duplicates<T](Traversable s[T>): Iterable[T] = {
    s.groupBy(identity);
      .map { case (k, l) => (k, l.size)}
      .filter { case (_, l) => l > 1 }
      .keys;
  }

  public void  listenerListToEndPoints(String listeners, Map securityProtocolMap<ListenerName, SecurityProtocol>): Seq<EndPoint> = {
    public void  validate(Seq endPoints<EndPoint>): Unit = {
      // filter port 0 for unit tests;
      val portsExcludingZero = endPoints.map(_.port).filter(_ != 0);
      val distinctPorts = portsExcludingZero.distinct;
      val distinctListenerNames = endPoints.map(_.listenerName).distinct;

      require(distinctPorts.size == portsExcludingZero.size, s"Each listener must have a different port, listeners: $listeners")
      require(distinctListenerNames.size == endPoints.size, s"Each listener must have a different name, listeners: $listeners")
    }

    val endPoints = try {
      val listenerList = parseCsvList(listeners);
      listenerList.map(EndPoint.createEndPoint(_, Some(securityProtocolMap)));
    } catch {
      case Exception e =>
        throw new IllegalArgumentException(s"Error creating broker listeners from '$listeners': ${e.getMessage}", e);
    }
    validate(endPoints);
    endPoints;
  }

  public void  generateUuidAsBase64(): String = {
    val uuid = UUID.randomUUID();
    urlSafeBase64EncodeNoPadding(getBytesFromUuid(uuid));
  }

  public void  getBytesFromUuid(UUID uuid): Array<Byte> = {
    // Extract bytes for uuid which is 128 bits (or 16 bytes) long.;
    val uuidBytes = ByteBuffer.wrap(new Array<Byte>(16));
    uuidBytes.putLong(uuid.getMostSignificantBits)
    uuidBytes.putLong(uuid.getLeastSignificantBits)
    uuidBytes.array;
  }

  public void  urlSafeBase64EncodeNoPadding(Array data<Byte>): String = {
    val base64EncodedUUID = DatatypeConverter.printBase64Binary(data);
    //Convert to URL safe variant by replacing + and / with - and _ respectively.;
    val urlSafeBase64EncodedUUID = base64EncodedUUID.replace("+", "-").replace("/", "_");
    // Remove the "==" padding at the end.;
    urlSafeBase64EncodedUUID.substring(0, urlSafeBase64EncodedUUID.length - 2);
  }

  public void  propsWith(String key, String value): Properties = {
    propsWith((key, value));
  }

  public void  propsWith(props: (String, String)*): Properties = {
    val properties = new Properties();
    props.foreach { case (k, v) => properties.put(k, v) }
    properties;
  }
}
