/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.shell.command;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Loads a file or directory in Alluxio space, makes it resident in memory.
 */
@ThreadSafe
public final class ReadTestCommand extends WithWildCardPathCommand {

  /**
   * @param fs the filesystem of Alluxio
   */
  public ReadTestCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  public String getCommandName() {
    return "readTest";
  }

  @Override
  public Options getOptions() {
    return new Options()
        .addOption(Option.builder()
            .longOpt("position")
            .required(false)
            .hasArg(true)
            .desc("the read position.")
            .build())
        .addOption(Option.builder()
            .longOpt("len")
            .required(false)
            .hasArg(true)
            .desc("the len to read.")
            .build())
        .addOption(Option.builder()
            .longOpt("bufLen")
            .required(false)
            .hasArg(true)
            .desc("the buffer len.")
            .build())
        .addOption(Option.builder("A")
            .required(false)
            .hasArg(false)
            .desc("ReadAll.")
            .build())
        .addOption(Option.builder("V")
            .required(false)
            .hasArg(false)
            .desc("Verbose.")
            .build())
        .addOption(Option.builder("R")
            .required(false)
            .hasArg(false)
            .desc("Random.")
            .build());
  }

  @Override
  protected void runCommand(AlluxioURI path, CommandLine cl) throws AlluxioException, IOException {

    int bufLen = 8 * Constants.MB;

    if (cl.hasOption("bufLen")) {
      bufLen = Integer.parseInt(cl.getOptionValue("bufLen"));
    }


    long total = 0;
    int warmUp = 3;
    int iteration = 10;

    for (int i = -warmUp; i < iteration; i ++) {
      if (cl.hasOption("R")) {
        total += randRead(path, bufLen, cl.hasOption("V"), i);
      } else {
        total += seqRead(path, bufLen, cl.hasOption("V"), i);
      }
    }
    System.out.println(path + " " + bufLen + " " + total / iteration);
  }

  private long randRead(AlluxioURI path, int bufLen, boolean verbose, int i)
      throws IOException {
    Path fsPath = new Path(path.toString());
    Configuration conf = new Configuration();
    conf.set("fs.alluxio.impl", "alluxio.hadoop.FileSystem");
    conf.set("fs.AbstractFileSystem.alluxio.impl", "alluxio.hadoop.AlluxioFileSystem");
    org.apache.hadoop.fs.FileSystem fileSystem = fsPath.getFileSystem(conf);
    FSDataInputStream inputStream = null;

    try {
      inputStream = fileSystem.open(fsPath);
      FileStatus fileStatus = fileSystem.getFileStatus(fsPath);
      byte[] buffer = new byte[bufLen];
      long itr = fileStatus.getLen() / bufLen;
      long startTime = System.currentTimeMillis();

      for (int j = 0; j < itr; j ++) {
        long posIndex = ThreadLocalRandom.current().nextLong(0, itr);
        long position = posIndex * bufLen;
        inputStream.read(position, buffer, 0, bufLen);
      }

      long duration = System.currentTimeMillis() - startTime;
      if (verbose) {
        System.out.println((i < 0 ? "[warmup]" : "") + "time: " + duration);
      }
      if (i >= 0) {
        return duration;
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (inputStream != null) {
        inputStream.close();
      }
    }
    return 0;
}

  private long seqRead(AlluxioURI path, int bufLen, boolean verbose, int i)
      throws IOException {
    Path fsPath = new Path(path.toString());
    Configuration conf = new Configuration();
    conf.set("fs.alluxio.impl", "alluxio.hadoop.FileSystem");
    conf.set("fs.AbstractFileSystem.alluxio.impl", "alluxio.hadoop.AlluxioFileSystem");
    org.apache.hadoop.fs.FileSystem fileSystem = fsPath.getFileSystem(conf);
    FSDataInputStream inputStream = null;
    long position = 0;
    try {
      inputStream = fileSystem.open(fsPath);
      FileStatus fileStatus = fileSystem.getFileStatus(fsPath);
      byte[] buffer = new byte[bufLen];
      long readLen = fileStatus.getLen() - position;
      long startTime = System.currentTimeMillis();
      while (position < readLen) {
        long curReadLen =
            (readLen - position) > buffer.length ? buffer.length : (readLen - position);
        inputStream.read(position, buffer, 0, (int) curReadLen);
        position += curReadLen;
      }
      long duration = System.currentTimeMillis() - startTime;
      if (verbose) {
        System.out.println((i < 0 ? "[warmup]" : "") + "time: " + duration);
      }
      if (i >= 0) {
        return duration;
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (inputStream != null) {
        inputStream.close();
      }
    }
    return 0;
  }

  @Override
  public String getUsage() {
    return "readTest [--position=POST] <path>";
  }

  @Override
  public String getDescription() {
    return "test read files in Alluxio space, give a test report.";
  }

}
