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
package kafka.server.checkpoints;

import java.io._;
import java.nio.charset.StandardCharsets;
import java.nio.file.{FileAlreadyExistsException, FileSystems, Files, Paths}
import kafka.utils.{Exit, Logging}
import org.apache.kafka.common.utils.Utils;
import scala.collection.{Seq, mutable}

trait CheckpointFileFormatter[T]{
  public void  toLine(T entry): String;

  public void  fromLine(String line): Option[T];
}

class CheckpointFile[T](val File file, Integer version, CheckpointFileFormatter formatter[T]) extends Logging {
  private val path = file.toPath.toAbsolutePath;
  private val tempPath = Paths.get(path.toString + ".tmp");
  private val lock = new Object();
  ;
  try Files.createFile(file.toPath) // create the file if it doesn't exist;
  catch { case FileAlreadyExistsException _ => }

  public void  write(Seq entries[T]) {
    lock synchronized {
      // write to temp file and then swap with the existing file;
      val fileOutputStream = new FileOutputStream(tempPath.toFile);
      val writer = new BufferedWriter(new OutputStreamWriter(fileOutputStream, StandardCharsets.UTF_8));
      try {
        writer.write(version.toString);
        writer.newLine();

        writer.write(entries.size.toString);
        writer.newLine();

        entries.foreach { entry =>
          writer.write(formatter.toLine(entry))
          writer.newLine();
        }

        writer.flush();
        fileOutputStream.getFD().sync();
      } catch {
        case FileNotFoundException e =>
          if (FileSystems.getDefault.isReadOnly) {
            fatal(s"Halting writes to checkpoint file (${file.getAbsolutePath}) because the underlying file system is inaccessible: ", e);
            Exit.halt(1);
          }
          throw e;
      } finally {
        writer.close();
      }

      Utils.atomicMoveWithFallback(tempPath, path);
    }
  }

  public void  read(): Seq[T] = {
    public void  malformedLineException(String line) =
      new IOException(s"Malformed line in checkpoint file (${file.getAbsolutePath}): $line'")

    lock synchronized {
      val reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8));
      var String line = null;
      try {
        line = reader.readLine();
        if (line == null)
          return Seq.empty;
        line.toInt match {
          case fileVersion if fileVersion == version =>
            line = reader.readLine();
            if (line == null)
              return Seq.empty;
            val expectedSize = line.toInt;
            val entries = mutable.Buffer[T]();
            line = reader.readLine();
            while (line != null) {
              val entry = formatter.fromLine(line)
              entry match {
                case Some(e) =>
                  entries += e;
                  line = reader.readLine();
                case _ => throw malformedLineException(line)
              }
            }
            if (entries.size != expectedSize)
              throw new IOException(s"Expected $expectedSize entries in checkpoint file (${file.getAbsolutePath}), but found only ${entries.size}");
            entries;
          case _ =>
            throw new IOException(s"Unrecognized version of the checkpoint file (${file.getAbsolutePath}): " + version);
        }
      } catch {
        case NumberFormatException _ => throw malformedLineException(line)
      } finally {
        reader.close();
      }
    }
  }
}