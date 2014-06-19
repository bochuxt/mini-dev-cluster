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

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.ShuffleHandler;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.NodeHealthCheckerService;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdater;
import org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdaterImpl;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceTrackerService;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptRegistrationEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptUnregistrationEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

/**
 * COPIED FROM APACHE with minor modification mainly to allow bootstrapping from
 * provided configuration file.
 *
 */
public class MiniHadoopCluster extends CompositeService {

  private static final Log logger = LogFactory.getLog(MiniHadoopCluster.class);

  private final ResourceManager resourceManager;

  private final NodeManager[] nodeManagers;

  // Number of nm-local-dirs per nodemanager
  private final int numLocalDirs;
  // Number of nm-log-dirs per nodemanager
  private final int numLogDirs;

  private File testWorkDir;

  private final YarnConfiguration configuration;

  private MiniDFSCluster dfsCluster;

  /**
   *
   * @param clusterName
   * @param numNodeManagers
   */
  public MiniHadoopCluster(String clusterName, int numNodeManagers) {
    super(clusterName);
    this.testWorkDir = new File("target/MINI_YARN_CLUSTER");
    this.resourceManager = new UnsecureResourceManager();
    this.numLocalDirs = 1;
    this.numLogDirs = 1;
    this.nodeManagers = new NodeManager[numNodeManagers];
    this.configuration = new YarnConfiguration(new Configuration());
    this.configuration.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class, ResourceScheduler.class);
    this.configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, "target/MINI_DFS_CLUSTER/data");
    try {
      FileUtils.deleteDirectory(MiniHadoopCluster.this.testWorkDir);
    }
    catch (Exception e) {
      logger.warn("Failed to remove 'target' directory", e);
    }
  }

  @Override
  public void start() {
    try {
      if (logger.isInfoEnabled()){
        logger.info("Starting DFS Cluster");
      }
      this.dfsCluster =
          new MiniDFSCluster.Builder(this.configuration).numDataNodes(this.nodeManagers.length).nameNodePort(55555).build();

      FileSystem fs = this.dfsCluster.getFileSystem();
      if (logger.isInfoEnabled()){
        logger.info("Created default FileSystem at: " + fs.getUri().toString());
      }
      this.configuration.set("fs.defaultFS", fs.getUri().toString()); // use HDFS
      this.configuration.setInt("yarn.nodemanager.delete.debug-delay-sec", 60000);
    }
    catch (Exception e) {
      throw new IllegalStateException("Failed to start DFS cluster", e);
    }

    this.init(this.configuration);
    super.start();
  }

  @Override
  public void stop() {
    for (NodeManager nm : this.nodeManagers) {
      if (logger.isInfoEnabled()){
        logger.info("Stopping Node Manager: " + nm);
      }
      nm.stop();
    }
    if (logger.isInfoEnabled()){
      logger.info("Stopping Resource Manager: " + this.resourceManager);
    }
    this.resourceManager.stop();
    if (logger.isInfoEnabled()){
      logger.info("Stopping DFS Cluster");
    }
    this.dfsCluster.shutdown();
  }

  /**
	 *
	 */
  @Override
  public void serviceInit(Configuration conf) throws Exception {
    conf.setBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, true);
    conf.setStrings(YarnConfiguration.NM_AUX_SERVICES,
      new String[] { ShuffleHandler.MAPREDUCE_SHUFFLE_SERVICEID });
    conf.setClass(String.format(YarnConfiguration.NM_AUX_SERVICE_FMT,
      ShuffleHandler.MAPREDUCE_SHUFFLE_SERVICEID), ShuffleHandler.class, Service.class);
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, 0);

    this.addService(new ResourceManagerWrapper());
    for (int index = 0; index < this.nodeManagers.length; index++) {
      this.nodeManagers[index] = new ShortCircuitedNodeManager();
      this.addService(new NodeManagerWrapper(index));
    }
    super.serviceInit(conf instanceof YarnConfiguration
      ? conf
      : new YarnConfiguration(conf));
  }

  /**
	 *
	 */
  private class UnsecureResourceManager extends ResourceManager {
    @Override
    protected void doSecureLogin() throws IOException {
      // Don't try to login using keytab in the testcases.
    }
  }

  /**
	 *
	 */
  private class ResourceManagerWrapper extends AbstractService {

    public ResourceManagerWrapper() {
      super(ResourceManagerWrapper.class.getName());
    }

    @Override
    protected synchronized void serviceInit(Configuration conf) throws Exception {
      this.initResourceManager(conf);
      super.serviceInit(conf);
    }

    @Override
    protected synchronized void serviceStart() throws Exception {
      MiniHadoopCluster.this.resourceManager.start();
      if (logger.isInfoEnabled()){
        logger.info("MiniYARN ResourceManager address: "
            + getConfig().get(YarnConfiguration.RM_ADDRESS));
        logger.info("MiniYARN ResourceManager web address: "
            + WebAppUtils.getRMWebAppURLWithoutScheme(getConfig()));
      }
    }

    @Override
    protected synchronized void serviceStop() throws Exception {
      if (MiniHadoopCluster.this.resourceManager != null) {
        MiniHadoopCluster.this.resourceManager.stop();
      }
      super.serviceStop();
    }

    /**
     *
     * @param conf
     */
    private synchronized void initResourceManager(Configuration conf) {
      MiniHadoopCluster.this.resourceManager.init(conf);
      MiniHadoopCluster.this.resourceManager.getRMContext().getDispatcher().register(RMAppAttemptEventType.class,
          new EventHandler<RMAppAttemptEvent>() {
            @Override
            public void handle(RMAppAttemptEvent event) {
              if (event instanceof RMAppAttemptRegistrationEvent) {
                if (logger.isInfoEnabled()){
                  logger.info("Registered AM for " + event.getApplicationAttemptId());
                }
              }
              else if (event instanceof RMAppAttemptUnregistrationEvent) {
                if (logger.isInfoEnabled()){
                  logger.info("Un-Registered AM for " + event.getApplicationAttemptId());
                }
              }
            }
          });
    }
  }

  /**
	 *
	 */
  private class NodeManagerWrapper extends AbstractService {
    int index = 0;

    public NodeManagerWrapper(int i) {
      super(NodeManagerWrapper.class.getName() + "_" + i);
      this.index = i;
    }

    @Override
    protected synchronized void serviceInit(Configuration conf) throws Exception {
      Configuration config = new YarnConfiguration(conf);
      // create nm-local-dirs and configure them for the nodemanager
      String localDirsString = prepareDirs("local", MiniHadoopCluster.this.numLocalDirs);
      config.set(YarnConfiguration.NM_LOCAL_DIRS, localDirsString);
      // create nm-log-dirs and configure them for the nodemanager
      String logDirsString = prepareDirs("log", numLogDirs);
      config.set(YarnConfiguration.NM_LOG_DIRS, logDirsString);

      File remoteLogDir = new File(MiniHadoopCluster.this.testWorkDir,
            MiniHadoopCluster.this.getName() + "-remoteLogDir-nm-" + this.index);
      remoteLogDir.mkdir();
      if (!remoteLogDir.exists()) {
        throw new IllegalStateException("Failed to make "
            + remoteLogDir.getAbsolutePath() + " directory");
      }
      config.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
        remoteLogDir.getAbsolutePath());
      config.setInt(YarnConfiguration.NM_PMEM_MB, 4 * 1024);
      config.set(YarnConfiguration.NM_ADDRESS,
        this.getConfig().get("yarn.resourcemanager.hostname") + ":0");
      config.set(YarnConfiguration.NM_LOCALIZER_ADDRESS,
        this.getConfig().get("yarn.resourcemanager.hostname") + ":0");
      WebAppUtils.setNMWebAppHostNameAndPort(config,
        this.getConfig().get("yarn.resourcemanager.hostname"), 0);

      // Disable resource checks by default
      if (!config.getBoolean(
        YarnConfiguration.YARN_MINICLUSTER_CONTROL_RESOURCE_MONITORING,
        YarnConfiguration.DEFAULT_YARN_MINICLUSTER_CONTROL_RESOURCE_MONITORING)) {
        config.setBoolean(YarnConfiguration.NM_PMEM_CHECK_ENABLED, false);
        config.setBoolean(YarnConfiguration.NM_VMEM_CHECK_ENABLED, false);
      }

      if (logger.isInfoEnabled()){
        logger.info("Starting NM: " + this.index);
      }
      MiniHadoopCluster.this.nodeManagers[this.index].init(config);
      super.serviceInit(config);
    }

    /**
     * Create local/log directories
     *
     * @param dirType
     *          type of directories i.e. local dirs or log dirs
     * @param numDirs
     *          number of directories
     * @return the created directories as a comma delimited String
     */
    private String prepareDirs(String dirType, int numDirs) {
      File[] dirs = new File[numDirs];
      String dirsString = "";
      for (int i = 0; i < numDirs; i++) {
        dirs[i] = new File(MiniHadoopCluster.this.testWorkDir,
              MiniHadoopCluster.this.getName() + "-" + dirType + "Dir-nm-" + this.index + "_" + i);
        dirs[i].mkdirs();
        if (logger.isDebugEnabled()){
          logger.info("Created " + dirType + "Dir in " + dirs[i].getAbsolutePath());
        }
        String delimiter = (i > 0) ? "," : "";
        dirsString = dirsString.concat(delimiter + dirs[i].getAbsolutePath());
      }
      return dirsString;
    }

    @Override
    protected synchronized void serviceStart() throws Exception {
      MiniHadoopCluster.this.nodeManagers[this.index].start();
    }

    @Override
    protected synchronized void serviceStop() throws Exception {
      if (MiniHadoopCluster.this.nodeManagers[this.index] != null) {
        MiniHadoopCluster.this.nodeManagers[this.index].stop();
      }
      super.serviceStop();
    }
  }

  /**
	 *
	 */
  private class ShortCircuitedNodeManager extends NodeManager {
    @Override
    protected NodeStatusUpdater createNodeStatusUpdater(Context context,
        Dispatcher dispatcher, NodeHealthCheckerService healthChecker) {
      return new NodeStatusUpdaterImpl(context, dispatcher, healthChecker, this.metrics) {
        @Override
        protected ResourceTracker getRMClient() {
          final ResourceTrackerService rt = MiniHadoopCluster.this.resourceManager
                .getResourceTrackerService();
          // For in-process communication without RPC
          return new ResourceTracker() {

            @Override
            public NodeHeartbeatResponse nodeHeartbeat(NodeHeartbeatRequest request) throws YarnException, IOException {
              NodeHeartbeatResponse response;
              try {
                response = rt.nodeHeartbeat(request);
              }
              catch (YarnException e) {
                logger.error("Exception in heartbeat from node "
                    + request.getNodeStatus().getNodeId(), e);
                throw e;
              }
              return response;
            }

            @Override
            public RegisterNodeManagerResponse registerNodeManager(RegisterNodeManagerRequest request) throws YarnException, IOException {
              RegisterNodeManagerResponse response;
              try {
                response = rt.registerNodeManager(request);
              }
              catch (YarnException e) {
                logger.error("Exception in node registration from "
                    + request.getNodeId().toString(), e);
                throw e;
              }
              return response;
            }
          };
        }

        @Override
        protected void stopRMProxy() {
          // ignore
        }
      };
    }
  }
}
