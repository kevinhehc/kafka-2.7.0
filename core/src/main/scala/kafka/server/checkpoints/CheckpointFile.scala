/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package kafka.server.checkpoints

import java.io._
import java.nio.charset.StandardCharsets
import java.nio.file.{FileAlreadyExistsException, Files, Paths}

import kafka.server.LogDirFailureChannel
import kafka.utils.Logging
import org.apache.kafka.common.errors.KafkaStorageException
import org.apache.kafka.common.utils.Utils

import scala.collection.{Seq, mutable}

trait CheckpointFileFormatter[T]{
  def toLine(entry: T): String

  def fromLine(line: String): Option[T]
}

class CheckpointReadBuffer[T](location: String,
                              reader: BufferedReader,
                              version: Int,
                              formatter: CheckpointFileFormatter[T]) extends Logging {
  def read(): Seq[T] = {
    def malformedLineException(line: String) =
      new IOException(s"Malformed line in checkpoint file ($location): '$line'")

    var line: String = null
    try {
      line = reader.readLine()
      if (line == null)
        return Seq.empty
      line.toInt match {
        case fileVersion if fileVersion == version =>
          line = reader.readLine()
          if (line == null)
            return Seq.empty
          val expectedSize = line.toInt
          val entries = mutable.Buffer[T]()
          line = reader.readLine()
          while (line != null) {
            val entry = formatter.fromLine(line)
            entry match {
              case Some(e) =>
                entries += e
                line = reader.readLine()
              case _ => throw malformedLineException(line)
            }
          }
          if (entries.size != expectedSize)
            throw new IOException(s"Expected $expectedSize entries in checkpoint file ($location), but found only ${entries.size}")
          entries
        case _ =>
          throw new IOException(s"Unrecognized version of the checkpoint file ($location): " + version)
      }
    } catch {
      case _: NumberFormatException => throw malformedLineException(line)
    }
  }
}

class CheckpointFile[T](val file: File,
                        version: Int,
                        formatter: CheckpointFileFormatter[T],
                        logDirFailureChannel: LogDirFailureChannel,
                        logDir: String) extends Logging {
  private val path = file.toPath.toAbsolutePath
  private val tempPath = Paths.get(path.toString + ".tmp")
  private val lock = new Object()

  try Files.createFile(file.toPath) // create the file if it doesn't exist
  catch { case _: FileAlreadyExistsException => }

  // 用于将接收到的数据写入到偏移量检查点文件中。
  def write(entries: Iterable[T]): Unit = {
    // 获取该对象的锁，以保证写操作的互斥性和可见性。
    lock synchronized {
      try {
        // write to temp file and then swap with the existing file
        // 创建一个文件输出流对象，用于写入偏移量检查点文件。
        val fileOutputStream = new FileOutputStream(tempPath.toFile)
        // 创建一个带有缓冲的输出流，用于写入偏移量检查点文件的每一行。
        val writer = new BufferedWriter(new OutputStreamWriter(fileOutputStream, StandardCharsets.UTF_8))
        try {
          // 将偏移量检查点文件的版本号写入到文件中。
          writer.write(version.toString)
          writer.newLine()

          // 将待写入的数据条数写入到文件中。
          writer.write(entries.size.toString)
          writer.newLine()

          // 对于待写入的每一个数据条目，循环使用 formatter 将其转化为文本格式，然后写入到文件中。
          entries.foreach { entry =>
            writer.write(formatter.toLine(entry))
            writer.newLine()
          }

          // 将输出流中的缓存内容刷新到磁盘，确保数据被写入到磁盘中。
          writer.flush()
          // 将文件所在的磁盘缓存与磁盘同步，以避免数据丢失。
          fileOutputStream.getFD().sync()
        } finally {
          writer.close()
        }

        // 将临时文件原子地移动到偏移量检查点文件中，以保证文件的一致性和可用性。
        Utils.atomicMoveWithFallback(tempPath, path)
      } catch {
        case e: IOException =>
          val msg = s"Error while writing to checkpoint file ${file.getAbsolutePath}"
          logDirFailureChannel.maybeAddOfflineLogDir(logDir, msg, e)
          throw new KafkaStorageException(msg, e)
      }
    }
  }

  def read(): Seq[T] = {
    lock synchronized {
      try {
        val reader = Files.newBufferedReader(path)
        try {
          val checkpointBuffer = new CheckpointReadBuffer[T](file.getAbsolutePath, reader, version, formatter)
          checkpointBuffer.read()
        } finally {
          reader.close()
        }
      } catch {
        case e: IOException =>
          val msg = s"Error while reading checkpoint file ${file.getAbsolutePath}"
          logDirFailureChannel.maybeAddOfflineLogDir(logDir, msg, e)
          throw new KafkaStorageException(msg, e)
      }
    }
  }
}
