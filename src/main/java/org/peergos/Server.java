package org.peergos;

import com.sun.net.httpserver.HttpServer;
import io.ipfs.multiaddr.MultiAddress;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.*;
import io.libp2p.protocol.Ping;
import org.peergos.blockstore.*;
import org.peergos.config.*;
import org.peergos.net.APIHandler;
import org.peergos.protocol.autonat.AutonatProtocol;
import org.peergos.protocol.bitswap.Bitswap;
import org.peergos.protocol.bitswap.BitswapEngine;
import org.peergos.protocol.circuit.CircuitHopProtocol;
import org.peergos.protocol.circuit.CircuitStopProtocol;
import org.peergos.protocol.dht.*;
import org.peergos.util.Logging;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Server {

    private static final Logger LOG = Logger.getLogger(Server.class.getName());

    private Blockstore buildBlockStore(Config config, Path blocksPath) {
        FileBlockstore blocks = new FileBlockstore(blocksPath);
        Blockstore blockStore = null;
        if (config.datastore.filter.type == FilterType.BLOOM) {
            blockStore = FilteredBlockstore.bloomBased(blocks, config.datastore.filter.falsePositiveRate);
        } else if (config.datastore.filter.type == FilterType.INFINI) {
            blockStore = FilteredBlockstore.infiniBased(blocks, config.datastore.filter.falsePositiveRate);
        } else if (config.datastore.filter.type == FilterType.NONE) {
            blockStore = blocks;
        } else {
            throw new IllegalStateException("Unhandled filter type: " + config.datastore.filter.type);
        }
        return config.datastore.allowedCodecs.codecs.isEmpty() ?
                blockStore : new TypeLimitedBlockstore(blockStore, config.datastore.allowedCodecs.codecs);
    }

    public Server() throws Exception {
        Path ipfsPath = getIPFSPath();
        Logging.init(ipfsPath);
        Config config = readConfig(ipfsPath);
        info("Starting Nabu version: " + APIService.CURRENT_VERSION);

        Path blocksPath = ipfsPath.resolve("blocks");
        File blocksDirectory = blocksPath.toFile();
        if (!blocksDirectory.exists()) {
            if (!blocksDirectory.mkdir()) {
                throw new IllegalStateException("Unable to make blocks directory");
            }
        } else if (blocksDirectory.isFile()) {
            throw new IllegalStateException("Unable to create blocks directory");
        }
        Blockstore blockStore = buildBlockStore(config, blocksPath);

        List<MultiAddress> swarmAddresses = config.addresses.getSwarmAddresses();
        int hostPort = swarmAddresses.get(0).getPort();
        HostBuilder builder = new HostBuilder().setIdentity(config.identity.privKeyProtobuf).listenLocalhost(hostPort);
        if (!builder.getPeerId().equals(config.identity.peerId)) {
            throw new IllegalStateException("PeerId invalid");
        }
        Multihash ourPeerId = Multihash.deserialize(builder.getPeerId().getBytes());

        Path datastorePath = ipfsPath.resolve("datastore").resolve("h2.datastore");
        DatabaseRecordStore records = new DatabaseRecordStore(datastorePath.toString());
        ProviderStore providers = new RamProviderStore();
        Kademlia dht = new Kademlia(new KademliaEngine(ourPeerId, providers, records), "/ipfs/kad/1.0.0", 20, 3, false);
        CircuitStopProtocol.Binding stop = new CircuitStopProtocol.Binding();
        CircuitHopProtocol.RelayManager relayManager = CircuitHopProtocol.RelayManager.limitTo(builder.getPrivateKey(), ourPeerId, 5);
        BlockRequestAuthoriser authoriser = (c, b, p, a) -> CompletableFuture.completedFuture(true);
        builder = builder.addProtocols(List.of(
                new Ping(),
                new AutonatProtocol.Binding(),
                new CircuitHopProtocol.Binding(relayManager, stop),
                new Bitswap(new BitswapEngine(blockStore, authoriser)),
                dht));

        Host node = builder.build();
        node.start().join();
        info("Node started and listening on " + node.listenAddresses());
        info("Starting bootstrap process");
        int connections = dht.bootstrapRoutingTable(node, config.bootstrap.getBootstrapAddresses(), addr -> !addr.contains("/wss/"));
        if (connections == 0)
            throw new IllegalStateException("No connected peers!");
        dht.bootstrap(node);

        MultiAddress apiAddress = config.addresses.apiAddress;
        InetSocketAddress localAPIAddress = new InetSocketAddress(apiAddress.getHost(), apiAddress.getPort());

        int maxConnectionQueue = 500;
        int handlerThreads = 50;
        info("Starting RPC API server at: localhost:" + localAPIAddress.getPort());
        HttpServer apiServer = HttpServer.create(localAPIAddress, maxConnectionQueue);

        APIService service = new APIService(blockStore, new BitswapBlockService(node, builder.getBitswap().get()));
        apiServer.createContext(APIService.API_URL, new APIHandler(service, node));
        apiServer.setExecutor(Executors.newFixedThreadPool(handlerThreads));
        apiServer.start();

        Thread shutdownHook = new Thread(() -> {
            info("Stopping server...");
            try {
                node.stop().get();
                apiServer.stop(3); //wait max 3 seconds
                records.close();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        });
        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }
    private void info(String message) {
        LOG.info(message);
        System.out.println(message);
    }
    private Path getIPFSPath() {
        String ipfsPath = System.getenv("IPFS_PATH");
        if (ipfsPath == null) {
            String home = System.getenv("HOME");
            return Path.of(home, ".ipfs");
        }
        return Path.of(ipfsPath);
    }

    private Config readConfig(Path configPath) throws IOException {
        Path configFilePath = configPath.resolve("config");
        File configFile = configFilePath.toFile();
        if (!configFile.exists()) {
            info("Unable to find config file. Creating default config");
            Config config = new Config();
            Files.write(configFilePath, config.toString().getBytes(), StandardOpenOption.CREATE);
            return config;
        }
        return Config.build(Files.readString(configFilePath));
    }

    public static void main(String[] args) {
        try {
            new Server();
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "SHUTDOWN", e);
        }
    }
}