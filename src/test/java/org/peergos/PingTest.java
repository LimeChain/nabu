package org.peergos;

import identify.pb.IdentifyOuterClass;
import io.ipfs.multiaddr.MultiAddress;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.Host;
import io.libp2p.core.crypto.PrivKey;
import io.libp2p.core.multiformats.Multiaddr;
import io.libp2p.core.multistream.ProtocolBinding;
import io.libp2p.crypto.keys.Ed25519Kt;
import io.libp2p.crypto.keys.RsaKt;
import io.libp2p.protocol.Identify;
import io.libp2p.protocol.IdentifyController;
import io.libp2p.protocol.Ping;
import io.libp2p.protocol.PingController;
import org.junit.Assert;
import org.junit.Test;
import org.peergos.blockstore.RamBlockstore;
import org.peergos.protocol.IdentifyBuilder;
import org.peergos.protocol.bitswap.Bitswap;
import org.peergos.protocol.bitswap.BitswapEngine;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class PingTest {

    @Test
    public void runPing() {
        Host node1 = HostBuilder.build(TestPorts.getPort(), List.of(new Ping(), new Bitswap(new BitswapEngine(new RamBlockstore(), (c, p, a) -> CompletableFuture.completedFuture(true), Bitswap.MAX_MESSAGE_SIZE))));
        Host node2 = HostBuilder.build(TestPorts.getPort(), List.of(new Ping(), new Bitswap(new BitswapEngine(new RamBlockstore(), (c, p, a) -> CompletableFuture.completedFuture(true), Bitswap.MAX_MESSAGE_SIZE))));
        node1.start().join();
        node2.start().join();
        try {
            Multiaddr address2 = node2.listenAddresses().get(0);
            PingController pinger = new Ping().dial(node1, address2).getController().join();

            System.out.println("Sending ping messages to " + address2);
            for (int i = 0; i < 2; i++) {
                long latency = pinger.ping().join();
                System.out.println("Ping " + i + ", latency " + latency + "ms");
            }
        } finally {
            node1.stop();
            node2.stop();
        }
    }

    public static Host build(PrivKey keys,
                             int listenPort,
                             List<ProtocolBinding> protocols) {
        return new HostBuilder()
                .setIdentity(keys.bytes())
                .listen(List.of(new MultiAddress("/ip4/0.0.0.0/tcp/" + listenPort)))
                .addProtocols(protocols)
                .build();
    }

    @Test
    public void runPingEd25519ToRSA() {
        PrivKey node1Keys = Ed25519Kt.generateEd25519KeyPair().getFirst();
        int node1Port = TestPorts.getPort();
        Host node1 = build(node1Keys, node1Port, List.of(new Ping(),
                new Bitswap(new BitswapEngine(new RamBlockstore(), (c, p, a) -> CompletableFuture.completedFuture(true), Bitswap.MAX_MESSAGE_SIZE))));
        PrivKey node2Keys = RsaKt.generateRsaKeyPair(2048).getFirst();
        int node2Port = TestPorts.getPort();
        Host node2 = build(node2Keys, node2Port, List.of(new Ping(),
                new Bitswap(new BitswapEngine(new RamBlockstore(), (c, p, a) -> CompletableFuture.completedFuture(true), Bitswap.MAX_MESSAGE_SIZE))));
        node1.start().join();
        node2.start().join();
        IdentifyBuilder.addIdentifyProtocol(node1, Collections.emptyList());
        IdentifyBuilder.addIdentifyProtocol(node2, Collections.emptyList());

        Assert.assertTrue(new Multihash(Multihash.Type.id, node1Keys.publicKey().bytes()).toString().equals(node1.getPeerId().toString()));
        Assert.assertTrue(new Multihash(Multihash.Type.sha2_256, Hash.sha256(node2Keys.publicKey().bytes())).toString().equals(node2.getPeerId().toString()));
        try {
            Multiaddr address2 = Multiaddr.fromString("/ip4/127.0.0.1/tcp/" + node2Port + "/p2p/" + node2.getPeerId());
            PingController pinger = new Ping().dial(node1, address2).getController().join();

            System.out.println("Sending ping messages to " + address2);
            for (int i = 0; i < 2; i++) {
                long latency = pinger.ping().join();
                System.out.println("Ping " + i + ", latency " + latency + "ms");
            }
        } finally {
            node1.stop();
            node2.stop();
        }
    }

    @Test
    public void replyIdentifyOnNewDial() {
        Host node1 = HostBuilder.build(TestPorts.getPort(), List.of(new Ping(), new Bitswap(new BitswapEngine(new RamBlockstore(), (c, p, a) -> CompletableFuture.completedFuture(true), Bitswap.MAX_MESSAGE_SIZE))));
        Host node2 = HostBuilder.build(TestPorts.getPort(), List.of(new Ping(), new Bitswap(new BitswapEngine(new RamBlockstore(), (c, p, a) -> CompletableFuture.completedFuture(true), Bitswap.MAX_MESSAGE_SIZE))));
        node1.start().join();
        IdentifyBuilder.addIdentifyProtocol(node1, Collections.emptyList());
        node2.start().join();
        IdentifyBuilder.addIdentifyProtocol(node2, Collections.emptyList());
        try {
            // ping from 1 to 2
            Multiaddr address2 = node2.listenAddresses().get(0);
            PingController pinger1 = new Ping().dial(node1, address2).getController().join();
            pinger1.ping().join();

            // identify from 2 to 1
            Multiaddr address1 = node1.listenAddresses().get(0);
            IdentifyController id2 = new Identify().dial(node2, address1).getController().join();
            IdentifyOuterClass.Identify idRes = id2.id().join();
        } finally {
            node1.stop();
            node2.stop();
        }
    }
}
