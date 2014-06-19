/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.minicluster;

import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author Oleg Zhurakousky
 *
 */
public class StartMiniHadoopCluster {

  private static final Log logger = LogFactory.getLog(StartMiniHadoopCluster.class);

  /**
   * MAKE SURE to start with enough memory -Xmx2048m -XX:MaxPermSize=256
   *
   * Will start a two node Hadoop DFS/YARN cluster
   *
   */
  public static void main(String[] args) throws Exception {
    System.out.println("=============== Starting TEZ MINI CLUSTER ===============");
    int nodeCount = 2;

    logger.info("Starting with " + nodeCount + " nodes");

    final MiniHadoopCluster yarnCluster =
        new MiniHadoopCluster("MINI_YARN_CLUSTER", nodeCount);

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        yarnCluster.stop();
        System.out.println("=============== Stopped TEZ MINI CLUSTER ===============");
      }
    });

    try {
      yarnCluster.start();
      System.out.println("######## MINI CLUSTER started on " + new Date() + ". ########");
      System.out.println("\t - YARN log and work directories are available in target/MINI_YARN_CLUSTER");
      System.out.println("\t - DFS log and work directories are available in target/MINI_DFS_CLUSTER");
    }
    catch (Exception e) {
      // won't actually shut down process. Need to see what's going on
      logger.error("Failed to start mini cluster", e);
      yarnCluster.close();
    }
  }
}
