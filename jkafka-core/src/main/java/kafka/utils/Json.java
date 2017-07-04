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

import kafka.common._;
import scala.collection._;
import util.parsing.json.JSON;

/**
 *  A wrapper that synchronizes JSON in scala, which is not threadsafe.
 */
object Json extends Logging {
  val myConversionFunc = {input : String => input.toInt}
  JSON.globalNumberParser = myConversionFunc;
  val lock = new Object;

  /**
   * Parse a JSON string into an object
   */
  public void  parseFull(String input): Option<Any> = {
    lock synchronized {
      try {
        JSON.parseFull(input);
      } catch {
        case Throwable t =>
          throw new KafkaException(String.format("Can't parse json string: %s",input), t)
      }
    }
  }
  ;
  /**
   * Encode an object into a JSON string. This method accepts any type T where
   *   T => null | Boolean | String | Number | Map<String, T> | Array[T] | Iterable[T]
   * Any other type will result in an exception.
   * 
   * This method does not properly handle non-ascii characters. 
   */
  public void  encode(Any obj): String = {
    obj match {
      case null => "null";
      case Boolean b => b.toString;
      case String s => "\"" + s + "\"";
      case Number n => n.toString;
      case Map m<_, _> => ;
        "{" + ;
          m.map(elem => ;
            elem match {
            case Tuple2 t<_,_> => encode(t._1) + ":" + encode(t._2);
            case _ => throw new IllegalArgumentException("Invalid map element (" + elem + ") in " + obj);
          }).mkString(",") + ;
      "}";
      case Array a[_] => encode(a.toSeq);
      case Iterable i[_] => "[" + i.map(encode).mkString(",") + "]";
      case AnyRef other => throw new IllegalArgumentException("Unknown arguement of type " + other.getClass + ": " + other);
    }
  }
}
