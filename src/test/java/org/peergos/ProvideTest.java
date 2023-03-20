package org.peergos;

import io.ipfs.cid.*;
import io.ipfs.multiaddr.*;
import io.ipfs.multihash.*;
import io.libp2p.core.*;
import org.junit.*;
import org.peergos.blockstore.*;
import org.peergos.protocol.dht.*;

import java.util.*;
import java.util.function.*;
import java.util.stream.*;

public class ProvideTest {

    @Test
    //@Ignore // until we can figure out NAT traversal and get a public ip
    public void provideBlock() {
        RamBlockstore blockstore = new RamBlockstore();
        HostBuilder builder1 = HostBuilder.build(10000 + new Random().nextInt(50000),
                new RamProviderStore(), new RamRecordStore(), blockstore);
        Host node1 = builder1.build();
        node1.start().join();
        Multihash node1Id = Multihash.deserialize(node1.getPeerId().getBytes());

        try {
            // Don't connect to local kubo
            List<MultiAddress> bootStrapNodes = List.of(
                            "/ip4/46.238.17.208/tcp/36015/p2p/12D3KooWM1uHVHfnxoyfDRGxBvEAxzL7hnadoRmgF1MrMCt6KvDv"
                    ).stream()
                    .map(MultiAddress::new)
                    .collect(Collectors.toList());
            Kademlia dht = builder1.getWanDht().get();
            Predicate<String> bootstrapAddrFilter = addr -> !addr.contains("/wss/"); // jvm-libp2p can't parse /wss addrs
            int connections = dht.bootstrapRoutingTable(node1, bootStrapNodes, bootstrapAddrFilter);
            if (connections == 0)
                throw new IllegalStateException("No connected peers!");
            dht.bootstrap(node1);

            // publish a block
            byte[] blockData = ("This is hopefully a unique block" + System.currentTimeMillis()).getBytes();
            Cid block = blockstore.put(blockData, Cid.Codec.Raw).join();
            PeerAddresses ourAddresses = new PeerAddresses(node1Id, node1.listenAddresses().stream()
                    .map(m -> new MultiAddress(m.toString()))
                    .collect(Collectors.toList()));
            dht.provideBlock(block, node1, ourAddresses).join();

            // retrieve our published block from kubo
            List<PeerAddresses> providers = dht.findProviders(block, node1, 10).join();
            List<PeerAddresses> withNode1 = providers.stream()
                    .filter(p -> p.peerId.equals(node1Id))
                    .collect(Collectors.toList());
            if (withNode1.isEmpty())
                throw new IllegalStateException("Couldn't find us as a provider of block!");
        } finally {
            node1.stop();
        }
    }
}
