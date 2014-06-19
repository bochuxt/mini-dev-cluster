package com.hortonworks.minicluster;

import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;

/**
 *
 * @author Oleg Zhurakousky
 *
 */
public class StartMiniHadoopCluster {

  private static final Log logger = LogFactory.getLog(StartMiniHadoopCluster.class);

  /**
   * MAKE SURE to start with enough memory -Xmx2048m -XX:MaxPermSize=256
   * @param args
   */
  public static void main(String[] args) throws Exception {
    System.out.println("=============== Starting TEZ MINI CLUSTER ===============");
    int nodeCount = 2;

    logger.info("Starting with " + nodeCount + " nodes");

    Configuration configuration = new Configuration();
    configuration.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class, ResourceScheduler.class);
    configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, "target/MINI_DFS_CLUSTER/data");
    final MiniDFSCluster dfsCluster =
        new MiniDFSCluster.Builder(configuration).numDataNodes(nodeCount)
          .nameNodePort(55555).build();

    FileSystem fs = dfsCluster.getFileSystem();
    System.out.println("Created default FileSystem at: " + fs.getUri().toString());

    configuration.set("fs.defaultFS", fs.getUri().toString()); // use HDFS
    configuration.setInt("yarn.nodemanager.delete.debug-delay-sec", 20000);

    YarnConfiguration yarnConfig = new YarnConfiguration();
    @SuppressWarnings("resource")
    final MiniHadoopCluster yarnCluster =
        new MiniHadoopCluster("MINI_YARN_CLUSTER", nodeCount);

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        yarnCluster.stop();
        dfsCluster.shutdown();
        System.out.println("=============== Stopped TEZ MINI CLUSTER ===============");
      }
    });

    try {
      yarnCluster.init(new YarnConfiguration(yarnConfig));
      yarnCluster.start();
      System.out.println("######## MINI CLUSTER started on " + new Date() + ". UI tracking is available at "
          + "http://localhost:8080/node/allApplications ########");
      System.out.println("\t - YARN log and work directories are available in target/MINI_YARN_CLUSTER");
      System.out.println("\t - DFS log and work directories are available in target/MINI_DFS_CLUSTER");
    }
    catch (Exception e) {
      // won't actually shut down process. Need to see what's going on
      logger.error("Failed to start mini cluster", e);
      yarnCluster.stop();
      dfsCluster.shutdown();
    }
  }
}
