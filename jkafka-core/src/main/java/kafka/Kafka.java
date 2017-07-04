///**
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// * 
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package kafka;
//
//import java.util.Properties;
//
//import joptsimple.OptionParser;
//import org.apache.kafka.common.utils.Utils;
//
//import scala.collection.JavaConverters._;
//
//public Class Kafka extends Logging {
//
//  public void  getPropsFromArgs(String[]  args): Properties = {
//    val optionParser = new OptionParser(false);
//    val overrideOpt = optionParser.accepts("override", "Optional property that should override values set in server.properties file");
//      .withRequiredArg();
//      .ofType(classOf<String>);
//
//    if (args.length == 0) {
//      CommandLineUtils.printUsageAndDie(optionParser, String.format("java USAGE <options> %s server.properties <--override property=value]*",classOf[KafkaServer>.getSimpleName()))
//    }
//
//    val props = Utils.loadProps(args(0));
//
//    if(args.length > 1) {
//      val options = optionParser.parse(args.slice(1, args.length): _*);
//
//      if(options.nonOptionArguments().size() > 0) {
//        CommandLineUtils.printUsageAndDie(optionParser, "Found non argument parameters: " + options.nonOptionArguments().toArray.mkString(","));
//      }
//
//      props.putAll(CommandLineUtils.parseKeyValueArgs(options.valuesOf(overrideOpt).asScala));
//    }
//    props;
//  }
//
//  public void  main(Array args<String>): Unit = {
//    try {
//      val serverProps = getPropsFromArgs(args);
//      val kafkaServerStartable = KafkaServerStartable.fromProps(serverProps);
//
//      // attach shutdown handler to catch control-c;
//      Runtime.getRuntime().addShutdownHook(new Thread("kafka-shutdown-hook") {
//        override public void  run(): Unit = kafkaServerStartable.shutdown();
//      });
//
//      kafkaServerStartable.startup();
//      kafkaServerStartable.awaitShutdown();
//    }
//    catch {
//       Throwable e =>
//        fatal(e);
//        Exit.exit(1);
//    }
//    Exit.exit(0);
//  }
//}
