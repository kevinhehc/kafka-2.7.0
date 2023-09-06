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

package kafka

import java.util.Properties

import joptsimple.OptionParser
import kafka.utils.Implicits._
import kafka.server.{KafkaServer, KafkaServerStartable}
import kafka.utils.{CommandLineUtils, Exit, Logging}
import org.apache.kafka.common.utils.{Java, LoggingSignalHandler, OperatingSystem, Utils}

import scala.jdk.CollectionConverters._

object Kafka extends Logging {

  def getPropsFromArgs(args: Array[String]): Properties = {
    // 创建一个命令行参数解析器
    val optionParser = new OptionParser(false)
    // 定义 --override 选项，用于覆盖 server.properties 文件中的属性
    val overrideOpt = optionParser.accepts("override", "Optional property that should override values set in server.properties file")
      .withRequiredArg()
      .ofType(classOf[String])
    // This is just to make the parameter show up in the help output, we are not actually using this due the
    // fact that this class ignores the first parameter which is interpreted as positional and mandatory
    // but would not be mandatory if --version is specified
    // This is a bit of an ugly crutch till we get a chance to rework the entire command line parsing
    // 定义 --version 选项，用于打印版本信息并退出
    optionParser.accepts("version", "Print version information and exit.")

    // 若没有提供参数或者参数包含 --help 选项，则打印用法并退出
    if (args.length == 0 || args.contains("--help")) {
      CommandLineUtils.printUsageAndDie(optionParser, "USAGE: java [options] %s server.properties [--override property=value]*".format(classOf[KafkaServer].getSimpleName()))
    }

    // 若参数中包含 --version 选项，则打印版本信息并退出
    if (args.contains("--version")) {
      CommandLineUtils.printVersionAndDie()
    }

    // 加载 server.properties 文件中的属性到 Properties 对象中
    val props = Utils.loadProps(args(0))

    // 若提供了其他参数，则解析这些参数
    if (args.length > 1) {
      // 解析参数中的选项和参数值
      val options = optionParser.parse(args.slice(1, args.length): _*)

      // 检查是否有非选项参数
      if (options.nonOptionArguments().size() > 0) {
        CommandLineUtils.printUsageAndDie(optionParser, "Found non argument parameters: " + options.nonOptionArguments().toArray.mkString(","))
      }

      // 将解析得到的选项和参数值添加到 props 对象中
      props ++= CommandLineUtils.parseKeyValueArgs(options.valuesOf(overrideOpt).asScala)
    }
    // 返回解析得到的属性集合
    props
  }

  def main(args: Array[String]): Unit = {
    try {
      // 解析命令行参数，获得属性集合
      val serverProps = getPropsFromArgs(args)
      // 从属性集合创建 KafkaServerStartable 对象
      val kafkaServerStartable = KafkaServerStartable.fromProps(serverProps)

      try {
        // 如果不是 Windows 操作系统，并且不是 IBM JDK，则注册 LoggingSignalHandler
        if (!OperatingSystem.IS_WINDOWS && !Java.isIbmJdk)
          new LoggingSignalHandler().register()
      } catch {
        // 如果注册 LoggingSignalHandler 失败，则在日志中打印警告信息
        case e: ReflectiveOperationException =>
          warn("Failed to register optional signal handler that logs a message when the process is terminated " +
            s"by a signal. Reason for registration failure is: $e", e)
      }

      // attach shutdown handler to catch terminating signals as well as normal termination
      // 添加 shutdown hook，用于在程序结束时执行 KafkaServerStartable 的 shutdown 方法
      Exit.addShutdownHook("kafka-shutdown-hook", kafkaServerStartable.shutdown())

      // 启动 KafkaServerStartable 实例
      kafkaServerStartable.startup()
      // 等待 KafkaServerStartable 实例终止
      kafkaServerStartable.awaitShutdown()
    }
    catch {
      // 如果有异常发生，则记录日志并退出程序
      case e: Throwable =>
        fatal("Exiting Kafka due to fatal exception", e)
        Exit.exit(1)
    }
    // 正常终止程序
    Exit.exit(0)
  }
}
