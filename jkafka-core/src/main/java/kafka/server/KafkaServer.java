package kafka.server;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import kafka.cluster.Broker;
import kafka.controller.KafkaController;
import kafka.coordinator.GroupCoordinator;
import kafka.log.CleanerConfig;
import kafka.log.LogConfig;
import kafka.log.LogManager;
import kafka.metrics.MetricConfig;
import kafka.metrics.Metrics;
import kafka.metrics.MetricsReporter;
import kafka.network.SocketServer;
import kafka.utils.KafkaScheduler;
import kafka.utils.SystemTime;
import kafka.utils.Time;
import kafka.utils.ZkUtils;
import kafka.xend.GuiceDI;

/**
 * 该类是kafka broker运行控制的核心入口类，它采用门面模式设计的
 * @author baodekang
 *
 */
public class KafkaServer {

    private Logger logger = LoggerFactory.getLogger(KafkaServer.class);

    private KafkaConfig config;

    private AtomicBoolean startupComplete = new AtomicBoolean(false);
    private AtomicBoolean isShuttingDown = new AtomicBoolean(false);
    private AtomicBoolean isStartingUp = new AtomicBoolean(false);

    private CountDownLatch shutdownLatch = new CountDownLatch(1);

    private static final String jmxPrefix = "kafka.server";

    private List<MetricsReporter> reporters = null;
    private Time kafkaMetricsTime;
    private Metrics metrics;

    private ZkUtils zkUtils;
    private KafkaScheduler kafkaScheduler;
    private SocketServer socketServer;
    private MetricConfig metricConfig;
    private LogManager logManager;
    private ReplicaManager replicaManager;
    private KafkaController kafkaController;
    private GroupCoordinator consumerCoordinator;
    private KafkaHealthcheck kafkaHealthcheck;
    private KafkaApis apis;

    private String logIdent;
    private String threadNamePrefix;
    private BrokerState brokerState;

    public KafkaServer() {
        try {
            PropertyConfigurator.configure("config/log4j.properties");
            GuiceDI.init();

            this.config = GuiceDI.getInstance(KafkaConfig.class);
            config.init();
            metricConfig = new MetricConfig().samples(config.metricNumSamples).timeWindow(config.metricSampleWindowMs, TimeUnit.MILLISECONDS);
            kafkaScheduler = new KafkaScheduler(10);
            kafkaMetricsTime = new SystemTime();
            reporters = new ArrayList<MetricsReporter>();
            brokerState = null;
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public void startup() throws Exception {
        logger.info("starting");
        if (isShuttingDown.get()) {
            throw new IllegalStateException("Kafka server is still shutting down, cannot re-start!");
        }

        if (startupComplete.get()) {
            return;
        }

        boolean canStartup = isStartingUp.compareAndSet(false, true);
        if (canStartup) {
            metrics = new Metrics(metricConfig, reporters, kafkaMetricsTime, true);

            /** 启动调度 */
            kafkaScheduler.startup();

            /** 启动zookeeper */
            initZk();
            GuiceDI.getInstance(ZkUtils.class).setupCommonPaths();
            GuiceDI.getInstance(ZkUtils.class).registerBrokerInZk(config.brokerId, config.hostName, config.port, //
                            6000, 0);

            /** 启动日志管理器 */
            logManager = createLogManager(null);
            logManager.startup();
            //   

            this.logIdent = "[Kafka Server " + config.brokerId + "],";

            socketServer = new SocketServer(config, metrics, kafkaMetricsTime);
            socketServer.startup();

            //            KafkaController kafkaController = new KafkaController(config, zkUtils, brokerState, kafkaMetricsTime, metrics, threadNamePrefix);
            //            kafkaController.startup();
            //   
            //   replicaManager = new ReplicaManager();
            //   replicaManager.startup();
            //   
            kafkaController = new KafkaController();
            kafkaController.startup();

            //   apis = new KafkaApis();

            //   consumerCoordinator = new GroupCoordinator();
            //   consumerCoordinator.startup();
            //   
            //   kafkaHealthcheck = new KafkaHealthcheck();
            //   kafkaHealthcheck.startup();
            //   
            //   shutdownLatch = new CountDownLatch(1);
            //   startupComplete.set(true);
            //   isStartingUp.set(true);
            //   AppInfoParser.registerAppInfo(jmxPrefix, String.valueOf(config.brokerId));
            //   logger.info("started");
        }
    }

    public void awaitShutdown() {
        System.out.println("kafka等待关闭");
    }

    public void shutdown() {
        System.out.println("关闭kafka...");
    }

    private void initZk() {
        logger.info("Connecting to zookeeper on ");

        String chroot = config.zkConnect.indexOf("/") > 0 ? config.zkConnect.substring(config.zkConnect.indexOf("/")) : "";
        //        boolean secureAclsEnabled = JaasUtils.isZkSecurityEnabled() && config.ZkEnableSecureAcls;
        //        if (config.ZkEnableSecureAcls && !secureAclsEnabled) {
        //            //        throw new java.lang.SecurityException("zkEnableSecureAcls is true, but the verification of the JAAS login file failed.");
        //        }

        ZkUtils zkUtils = GuiceDI.getInstance(ZkUtils.class);
        zkUtils.init(config.zkConnect, config.zkSessionTimeoutMs, 6000, false);
        if (chroot.length() > 1) {
            zkUtils.makeSurePersistentPathExists(chroot);
            //   logger.info("Created zookeeper path " + chroot);
        }
    }

    public LogManager createLogManager(BrokerState brokerState) {
        List<File> logDirs = Lists.newArrayList();
        for (String str : config.logDirs) {
            logDirs.add(new File(str));
        }
        Map<String, LogConfig> map = Maps.newHashMap();
        return new LogManager(logDirs, map, GuiceDI.getInstance(LogConfig.class), GuiceDI.getInstance(CleanerConfig.class), 3000L, 3000L, 6000L, kafkaScheduler, new SystemTime());
    }

}
