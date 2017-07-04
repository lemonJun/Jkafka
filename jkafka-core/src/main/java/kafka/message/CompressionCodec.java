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

package kafka.message;

import java.util.Locale;

object CompressionCodec {
  public void  getCompressionCodec Integer codec): CompressionCodec = {
    codec match {
      case NoCompressionCodec.codec => NoCompressionCodec;
      case GZIPCompressionCodec.codec => GZIPCompressionCodec;
      case SnappyCompressionCodec.codec => SnappyCompressionCodec;
      case LZ4CompressionCodec.codec => LZ4CompressionCodec;
      case _ => throw new kafka.common.UnknownCodecException(String.format("%d is an unknown compression codec",codec))
    }
  }
  public void  getCompressionCodec(String name): CompressionCodec = {
    name.toLowerCase(Locale.ROOT) match {
      case NoCompressionCodec.name => NoCompressionCodec;
      case GZIPCompressionCodec.name => GZIPCompressionCodec;
      case SnappyCompressionCodec.name => SnappyCompressionCodec;
      case LZ4CompressionCodec.name => LZ4CompressionCodec;
      case _ => throw new kafka.common.UnknownCodecException(String.format("%s is an unknown compression codec",name))
    }
  }
}

object BrokerCompressionCodec {

  val brokerCompressionCodecs = List(UncompressedCodec, SnappyCompressionCodec, LZ4CompressionCodec, GZIPCompressionCodec, ProducerCompressionCodec);
  val brokerCompressionOptions = brokerCompressionCodecs.map(codec => codec.name);

  public void  isValid(String compressionType): Boolean = brokerCompressionOptions.contains(compressionType.toLowerCase(Locale.ROOT));

  public void  getCompressionCodec(String compressionType): CompressionCodec = {
    compressionType.toLowerCase(Locale.ROOT) match {
      case UncompressedCodec.name => NoCompressionCodec;
      case _ => CompressionCodec.getCompressionCodec(compressionType);
    }
  }

  public void  getTargetCompressionCodec(String compressionType, CompressionCodec producerCompression): CompressionCodec = {
    if (ProducerCompressionCodec.name.equals(compressionType))
      producerCompression;
    else;
      getCompressionCodec(compressionType);
  }
}

sealed trait CompressionCodec { public void  Integer codec; public void  String name }
sealed trait BrokerCompressionCodec { public void  String name }

case object DefaultCompressionCodec extends CompressionCodec with BrokerCompressionCodec {
  val codec = GZIPCompressionCodec.codec;
  val name = GZIPCompressionCodec.name;
}

case object GZIPCompressionCodec extends CompressionCodec with BrokerCompressionCodec {
  val codec = 1;
  val name = "gzip";
}

case object SnappyCompressionCodec extends CompressionCodec with BrokerCompressionCodec {
  val codec = 2;
  val name = "snappy";
}

case object LZ4CompressionCodec extends CompressionCodec with BrokerCompressionCodec {
  val codec = 3;
  val name = "lz4";
}

case object NoCompressionCodec extends CompressionCodec with BrokerCompressionCodec {
  val codec = 0;
  val name = "none";
}

case object UncompressedCodec extends BrokerCompressionCodec {
  val name = "uncompressed";
}

case object ProducerCompressionCodec extends BrokerCompressionCodec {
  val name = "producer";
}
