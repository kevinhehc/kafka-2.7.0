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

package kafka.server

import java.util.Properties

import kafka.metrics.KafkaMetricsReporter
import kafka.utils.{Exit, Logging, VerifiableProperties}

import scala.collection.Seq

object KafkaServerStartable {
  def fromProps(serverProps: Properties): KafkaServerStartable = {
    fromProps(serverProps, None)
  }

  def fromProps(serverProps: Properties, threadNamePrefix: Option[String]): KafkaServerStartable = {
    val reporters = KafkaMetricsReporter.startReporters(new VerifiableProperties(serverProps))
    new KafkaServerStartable(KafkaConfig.fromProps(serverProps, false), reporters, threadNamePrefix)
  }
}

class KafkaServerStartable(val staticServerConfig: KafkaConfig, reporters: Seq[KafkaMetricsReporter], threadNamePrefix: Option[String] = None) extends Logging {
  // 创建 KafkaServer 实例
  // 构造函数有两个参数 —— staticServerConfig 表示静态服务器配置，reporters 表示 Kafka 指标报告器。
  // 如果 threadNamePrefix 参数未用于构造函数，则默认值为 None。threadNamePrefix 参数表示线程名称前缀，用于调试和维护目的。
  private val server = new KafkaServer(staticServerConfig, kafkaMetricsReporters = reporters, threadNamePrefix = threadNamePrefix)

  def this(serverConfig: KafkaConfig) = this(serverConfig, Seq.empty)

  // 启动 KafkaServer
  // startup 方法尝试启动 Kafka 服务器。如果启动 Kafka 服务器时发生异常，则记录一条 fatal 错误日志并退出程序。
  // 对于成功启动的 Kafka 服务器，它将开始监听客户端连接，并在收到消息时执行所需的操作。
  def startup(): Unit = {
    try server.startup()
    catch {
      // 如果出现异常，则记录日志并退出程序
      case _: Throwable =>
        // KafkaServer.startup() calls shutdown() in case of exceptions, so we invoke `exit` to set the status code
        fatal("Exiting Kafka.")
        Exit.exit(1)
    }
  }

  // 关闭 KafkaServer
  // shutdown 方法尝试停止 Kafka 服务器。如果在停止服务器时出现异常，则记录一条 fatal 错误日志并强制退出程序。
  // 调用 shutdown 方法后，服务器将不再接受新的请求，并开始等待当前进行中的请求完成。当所有处理中的请求都完成后，服务器将彻底停止。
  def shutdown(): Unit = {
    try server.shutdown()
    catch {
      // 如果出现异常，则记录日志并强制退出程序
      case _: Throwable =>
        fatal("Halting Kafka.")
        // Calling exit() can lead to deadlock as exit() can be called multiple times. Force exit.
        Exit.halt(1)
    }
  }

  /**
   * Allow setting broker state from the startable.
   * This is needed when a custom kafka server startable want to emit new states that it introduces.
   */
  // setServerState 方法允许从 KafkaServerStartable 对象中设置 broker 状态。
  // 如果自定义 KafkaServerStartable 对象想要引入新的状态，则此方法很有用。
  def setServerState(newState: Byte): Unit = {
    server.brokerState.newState(newState)
  }

  // 等待 KafkaServer 退出
  // awaitShutdown 方法等待 Kafka 服务器完全退出。
  // 在 Kafka 服务器执行 shutdown 方法后，它将不再接受新的请求。但是，服务器可能仍在处理一些已经接收的请求。
  // awaitShutdown 方法将阻塞当前线程，直到服务器彻底停止。
  def awaitShutdown(): Unit = server.awaitShutdown()

}


