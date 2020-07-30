package org.whispersystems.textsecuregcm.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import redis.embedded.RedisServer;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assume.assumeFalse;

/**
 * An abstract base class that assembles a real (local!) Redis cluster and provides a client to that cluster for
 * subclasses.
 */
public abstract class AbstractRedisClusterTest {

    private static final int MAX_SLOT   = 16384;
    private static final int NODE_COUNT = 2;

    private static RedisServer[] clusterNodes;

    private FaultTolerantRedisCluster redisCluster;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        assumeFalse(System.getProperty("os.name").equalsIgnoreCase("windows"));

        clusterNodes = new RedisServer[NODE_COUNT];

        for (int i = 0; i < NODE_COUNT; i++) {
            clusterNodes[i] = buildClusterNode(getNextRedisClusterPort());
            clusterNodes[i].start();
        }

        assembleCluster(clusterNodes);
    }

    @Before
    public void setUp() throws Exception {
        final List<String> urls = Arrays.stream(clusterNodes)
                                        .map(node -> String.format("redis://127.0.0.1:%d", node.ports().get(0)))
                                        .collect(Collectors.toList());

        redisCluster = new FaultTolerantRedisCluster("test-cluster", urls, Duration.ofSeconds(2), new CircuitBreakerConfiguration());
    }

    protected FaultTolerantRedisCluster getRedisCluster() {
        return redisCluster;
    }

    @After
    public void tearDown() throws Exception {
        redisCluster.shutdown();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        for (final RedisServer node : clusterNodes) {
            node.stop();
        }
    }

    private static RedisServer buildClusterNode(final int port) throws IOException, URISyntaxException {
        final File clusterConfigFile     = File.createTempFile("redis", ".conf");
        final File rdbFile               = File.createTempFile("redis", ".rdb");

        // Redis struggles with existing-but-empty RDB files
        rdbFile.delete();
        rdbFile.deleteOnExit();
        clusterConfigFile.deleteOnExit();

        return RedisServer.builder()
                .setting("cluster-enabled yes")
                .setting("cluster-config-file " + clusterConfigFile.getAbsolutePath())
                .setting("cluster-node-timeout 5000")
                .setting("dir " + System.getProperty("java.io.tmpdir"))
                .setting("dbfilename " + rdbFile.getName())
                .port(port)
                .build();
    }

    private static void assembleCluster(final RedisServer... nodes) throws InterruptedException {
        final RedisClient meetClient = RedisClient.create(RedisURI.create("127.0.0.1", nodes[0].ports().get(0)));

        try {
            final StatefulRedisConnection<String, String> connection = meetClient.connect();
            final RedisCommands<String, String>           commands   = connection.sync();

            for (int i = 1; i < nodes.length; i++) {
                commands.clusterMeet("127.0.0.1", nodes[i].ports().get(0));
            }
        } finally {
            meetClient.shutdown();
        }

        final int slotsPerNode = MAX_SLOT / nodes.length;

        for (int i = 0; i < nodes.length; i++) {
            final int startInclusive = i * slotsPerNode;
            final int endExclusive   = i == nodes.length - 1 ? MAX_SLOT : (i + 1) * slotsPerNode;

            final RedisClient assignSlotClient = RedisClient.create(RedisURI.create("127.0.0.1", nodes[i].ports().get(0)));

            try {
                final int[] slots = new int[endExclusive - startInclusive];

                for (int s = startInclusive; s < endExclusive; s++) {
                    slots[s - startInclusive] = s;
                }

                assignSlotClient.connect().sync().clusterAddSlots(slots);
            } finally {
                assignSlotClient.shutdown();
            }
        }

        final RedisClient                             waitClient = RedisClient.create(RedisURI.create("127.0.0.1", nodes[0].ports().get(0)));
        final StatefulRedisConnection<String, String> connection = waitClient.connect();

        try {
            // CLUSTER INFO gives us a big blob of key-value pairs, but the one we're interested in is `cluster_state`.
            // According to https://redis.io/commands/cluster-info, `cluster_state:ok` means that the node is ready to
            // receive queries, all slots are assigned, and a majority of master nodes are reachable.
            while (!connection.sync().clusterInfo().contains("cluster_state:ok")) {
                Thread.sleep(500);
            }
        } finally {
            waitClient.shutdown();
        }
    }

    public static int getNextRedisClusterPort() throws IOException {
        final int MAX_ITERATIONS = 11_000;
        int port;
        for (int i = 0; i < MAX_ITERATIONS; i++) {
            try (ServerSocket socket = new ServerSocket(0)) {
                socket.setReuseAddress(false);
                port = socket.getLocalPort();
            }
            if (port < 55535) {
                return port;
            }
        }
        throw new IOException("Couldn't find an open port below 55,535 in " + MAX_ITERATIONS + " tries");
    }
}
