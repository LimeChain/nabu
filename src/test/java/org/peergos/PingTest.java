package org.peergos;

import io.ipfs.multiaddr.MultiAddress;
import io.libp2p.core.*;
import io.libp2p.core.multiformats.*;
import io.libp2p.protocol.*;
import org.junit.*;
import org.peergos.blockstore.*;
import org.peergos.protocol.bitswap.*;

import java.util.*;

public class PingTest {

    @Test
    public void runPing() {
        Host node1 = HostBuilder.build(11001, List.of(new Ping(), new Bitswap(new BitswapEngine(new RamBlockstore()))));
        Host node2 = HostBuilder.build(11002, List.of(new Ping(), new Bitswap(new BitswapEngine(new RamBlockstore()))));
        node1.start().join();
        node2.start().join();
        for (MultiAddress addr : BootstrapTest.BOOTSTRAP_NODES) {
            String address = addr.toString();
            try {
                System.out.println("Trying to ping: ");
                System.out.println(address);
                Multiaddr address2 = new Multiaddr(address);//node2.listenAddresses().get(0);
                PingController pinger = new Ping().dial(node1, address2)
                        .getController()
                        .join();

                System.out.println("Sending ping messages to " + address2);
                for (int i = 0; i < 2; i++) {
                    long latency = pinger.ping().join();
                    System.out.println("Ping " + i + ", latency " + latency + "ms");
                }
            } catch (Exception e){
                System.out.println(e.getMessage());
            }
        }
        node1.stop();
        node2.stop();
    }
}
