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

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.takin.emmet.file.FileUtils;

/**
 * A file lock a la flock/funlock
 *
 * The given path will be created and opened if it doesn't exist.
 */
public class FileLock {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileLock.class);
    private File file;

    private FileChannel channel = null;
    private java.nio.channels.FileLock lock = null;

    public FileLock(String filename) {
        this(new File(filename));
    }

    public FileLock(File file) {
        this.file = file;
        FileUtils.createFileIfNotExist(file);
    }

    public synchronized void Lock() {
        try {
            Path path = Paths.get(file.getPath());
            if (channel != null && channel.isOpen()) {
                return;
            }
            channel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.READ);
            lock = channel.lock();
        } catch (IOException e) {
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException e1) {
                    LOGGER.error("file channel close failed.", e1);
                }
            }
        }
        return;
    }

    public synchronized boolean tryLock() {
        try {
            Path path = Paths.get(file.getPath());
            if (channel != null && channel.isOpen()) {
                return false;
            }
            channel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.READ);
            lock = channel.tryLock();
            if (lock != null) {
                return true;
            }
        } catch (IOException e) {
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException e1) {
                    LOGGER.error("file channel close failed.", e1);
                }
            }
            return false;
        }
        return false;
    }

    public synchronized void unlock() {
        try {
            if (lock != null) {
                lock.release();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (channel != null) {
                try {
                    channel.close(); // also releases the lock
                } catch (IOException e) {
                    LOGGER.error("file channel close failed.", e);
                }
            }
        }
    }

    public synchronized void destroy() {
        try {
            unlock();
            channel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
