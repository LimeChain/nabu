package org.peergos;

import io.ipfs.multiaddr.MultiAddress;
import io.libp2p.core.Host;
import io.libp2p.core.PeerId;
import io.libp2p.core.multiformats.Multiaddr;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.peergos.blockstore.RamBlockstore;
import org.peergos.protocol.circuit.CircuitHopProtocol;
import org.peergos.protocol.circuit.Relay;
import org.peergos.protocol.dht.Kademlia;
import org.peergos.protocol.dht.RamProviderStore;
import org.peergos.protocol.dht.RamRecordStore;

import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class RelayTest {

    @Test
    @Ignore // needs fixed find providers
    public void relay() {
        HostBuilder builder1 = HostBuilder.create(10000 + new Random().nextInt(50000),
                new RamProviderStore(1000), new RamRecordStore(), new RamBlockstore(), (c, p, a) -> CompletableFuture.completedFuture(true),false,  false);
        Host node1 = builder1.build();
        node1.start().join();
        HostBuilder builder2 = HostBuilder.create(10000 + new Random().nextInt(50000),
                new RamProviderStore(1000), new RamRecordStore(), new RamBlockstore(), (c, p, a) -> CompletableFuture.completedFuture(true),false,  false);
        Host node2 = builder2.build();
        node2.start().join();

        try {
            bootstrapNode(builder1, node1);
            bootstrapNode(builder2, node2);

            // set up node 2 to listen via a relay
            List<PeerAddresses> relays = Relay.findRelays(builder2.getWanDht().get(), node2);
            Assert.assertFalse("Relays found", relays.isEmpty());
            PeerAddresses relay = relays.get(0);
            Multiaddr relayAddr = Multiaddr.fromString(relay.getPublicAddresses().get(0).toString())
                    .withP2P(PeerId.fromBase58(relay.peerId.toBase58()));
            CircuitHopProtocol.HopController hop = builder2.getRelayHop().get().dial(node2, relayAddr).getController().join();
//            hop.reserve()

            // connect to node2 from node1 via a relay

        } finally {
            node1.stop();
        }
    }

    private static void bootstrapNode(HostBuilder builder, Host host) {
        // Don't connect to local kubo
        List<MultiAddress> bootStrapNodes = List.of(
                        "/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
                        "/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
                        "/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
                        "/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
                        "/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ", // mars.i.ipfs.io
                        "/ip4/104.131.131.82/tcp/4001/ipfs/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
                        "/ip4/104.236.179.241/tcp/4001/ipfs/QmSoLPppuBtQSGwKDZT2M73ULpjvfd3aZ6ha4oFGL1KrGM",
                        "/ip4/128.199.219.111/tcp/4001/ipfs/QmSoLSafTMBsPKadTEgaXctDQVcqN88CNLHXMkTNwMKPnu",
                        "/ip4/104.236.76.40/tcp/4001/ipfs/QmSoLV4Bbm51jM9C4gDYZQ9Cy3U6aXMJDAbzgu2fzaDs64",
                        "/ip4/178.62.158.247/tcp/4001/ipfs/QmSoLer265NRgSp2LA3dPaeykiS1J6DifTC88f5uVQKNAd",
                        "/ip6/2604:a880:1:20:0:0:203:d001/tcp/4001/ipfs/QmSoLPppuBtQSGwKDZT2M73ULpjvfd3aZ6ha4oFGL1KrGM",
                        "/ip6/2400:6180:0:d0:0:0:151:6001/tcp/4001/ipfs/QmSoLSafTMBsPKadTEgaXctDQVcqN88CNLHXMkTNwMKPnu",
                        "/ip6/2604:a880:800:10:0:0:4a:5001/tcp/4001/ipfs/QmSoLV4Bbm51jM9C4gDYZQ9Cy3U6aXMJDAbzgu2fzaDs64",
                        "/ip6/2a03:b0c0:0:1010:0:0:23:1001/tcp/4001/ipfs/QmSoLer265NRgSp2LA3dPaeykiS1J6DifTC88f5uVQKNAd"
                ).stream()
                .map(MultiAddress::new)
                .collect(Collectors.toList());
        Kademlia dht = builder.getWanDht().get();
        Predicate<String> bootstrapAddrFilter = addr -> !addr.contains("/wss/"); // jvm-libp2p can't parse /wss addrs
        int connections = dht.bootstrapRoutingTable(host, bootStrapNodes, bootstrapAddrFilter);
        if (connections == 0)
            throw new IllegalStateException("No connected peers!");
        dht.bootstrap(host);
    }
}
