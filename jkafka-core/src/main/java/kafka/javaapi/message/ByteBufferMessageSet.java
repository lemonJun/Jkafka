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
package kafka.javaapi.message;

import java.nio.ByteBuffer;

import kafka.common.LongRef;
import kafka.message._;

import scala.collection.JavaConverters._;

class ByteBufferMessageSet(val ByteBuffer buffer) extends MessageSet {
  private val kafka underlying.message.ByteBufferMessageSet = new kafka.message.ByteBufferMessageSet(buffer);
  ;
  public void  this(CompressionCodec compressionCodec, java messages.util.List<Message>) {
    this(new kafka.message.ByteBufferMessageSet(compressionCodec, new LongRef(0), messages._ asScala*).buffer);
  }

  public void  this(java messages.util.List<Message>) {
    this(NoCompressionCodec, messages);
  }

  public void  Integer validBytes = underlying.validBytes;

  public void  getBuffer = buffer;

  override public void  java iterator.util.Iterator<MessageAndOffset> = new java.util.Iterator<MessageAndOffset> {
    val underlyingIterator = underlying.iterator;
    override public void  hasNext(): Boolean = {
      underlyingIterator.hasNext;
    }

    override public void  next(): MessageAndOffset = {
      underlyingIterator.next;
    }

    override public void  remove = throw new UnsupportedOperationException("remove API on MessageSet is not supported");
  }

  override public void  String toString = underlying.toString;

  public void  Integer sizeInBytes = underlying.sizeInBytes;

  override public void  equals(Any other): Boolean = {
    other match {
      case ByteBufferMessageSet that => buffer.equals(that.buffer);
      case _ => false;
    }
  }


  override public void  Integer hashCode = buffer.hashCode;
}
