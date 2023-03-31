package org.peergos.protocol.dht;

import com.offbynull.kademlia.*;
import io.ipfs.multiaddr.*;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.*;
import io.libp2p.core.crypto.*;
import io.libp2p.core.multiformats.*;
import io.libp2p.core.multistream.*;
import io.libp2p.etc.types.*;
import io.libp2p.protocol.*;
import org.peergos.*;
import org.peergos.protocol.dnsaddr.*;
import org.peergos.protocol.ipns.*;

import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.util.logging.*;
import java.util.stream.*;
import java.util.stream.Stream;

public class Kademlia extends StrictProtocolBinding<KademliaController> implements AddressBookConsumer {

    private static final Logger LOG = Logger.getLogger(Kademlia.class.getName());
    public static final int BOOTSTRAP_PERIOD_MILLIS = 300_000;
    private final KademliaEngine engine;
    private final boolean localDht;
    private AddressBook addressBook;

    private final Integer replication;
    private final Integer alpha;

    public Kademlia(KademliaEngine dht, String protocolId, Integer replication, Integer alpha, boolean localDht) {
        super(protocolId, new KademliaProtocol(dht));
        this.engine = dht;
        this.localDht = localDht;
        this.replication = replication;
        this.alpha = alpha;
    }

    public void setAddressBook(AddressBook addrs) {
        engine.setAddressBook(addrs);
        this.addressBook = addrs;
    }

    public int bootstrapRoutingTable(Host host, List<MultiAddress> addrs, Predicate<String> filter) {
        List<String> resolved = addrs.stream()
                .parallel()
                .flatMap(a -> {
                    try {
                        return DnsAddr.resolve(a.toString()).stream();
                    } catch (CompletionException ce) {
                        ce.printStackTrace();
                        return Stream.empty();
                    }
                })
                .filter(filter)
                .collect(Collectors.toList());
        List<? extends CompletableFuture<? extends KademliaController>> futures = resolved.stream()
                .parallel()
                .map(addr -> dial(host, Multiaddr.fromString(addr)).getController())
                .collect(Collectors.toList());
        int successes = 0;
        for (CompletableFuture<? extends KademliaController> future : futures) {
            try {
                future.orTimeout(5, TimeUnit.SECONDS).join();
                successes++;
            } catch (Exception e) {
            }
        }
        return successes;
    }

    public void startBootstrapThread(Host us) {
        new Thread(() -> {
            while (true) {
                try {
                    bootstrap(us);
                    Thread.sleep(BOOTSTRAP_PERIOD_MILLIS);
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }
        }, "Kademlia bootstrap").start();
    }

    private boolean connectTo(Host us, PeerAddresses peer) {
        try {
            new Identify().dial(us, PeerId.fromBase58(peer.peerId.toBase58()), getPublic(peer)).getController().join().id().join();
            return true;
        } catch (Exception e) {
            if (e.getCause() instanceof NothingToCompleteException)
                LOG.info("Couldn't connect to " + peer.peerId);
            else
                e.printStackTrace();
            return false;
        }
    }

    public void bootstrap(Host us) {
        // lookup a random peer id
        byte[] hash = new byte[32];
        new Random().nextBytes(hash);
        Multihash randomPeerId = new Multihash(Multihash.Type.sha2_256, hash);
        findClosestPeers(randomPeerId, 20, us);

        // lookup our own peer id to keep our nearest neighbours up-to-date,
        // and connect to all of them, so they know about our addresses
        List<PeerAddresses> closestToUs = findClosestPeers(Multihash.deserialize(us.getPeerId().getBytes()), 20, us);
        int connectedClosest = 0;
        for (PeerAddresses peer : closestToUs) {
            if (connectTo(us, peer))
                connectedClosest++;
        }
        LOG.info("Bootstrap connected to " + connectedClosest + " nodes close to us.");
    }

    private int compareKeys(RoutingEntry a, RoutingEntry b, Id keyId) {
        int prefixDiff = b.key().getSharedPrefixLength(keyId) - a.key().getSharedPrefixLength(keyId);
        if (prefixDiff != 0)
            return prefixDiff;
        return a.addresses().peerId.toBase58().compareTo(b.addresses().peerId.toBase58());
    }

    public List<PeerAddresses> findClosestPeers(Multihash peerIdkey, int maxCount, Host us) {
        byte[] key = peerIdkey.toBytes();
        Id keyId = Id.create(Hash.sha256(key), 256);
        SortedSet<RoutingEntry> closest = new TreeSet<>((a, b) -> compareKeys(a, b, keyId));
        SortedSet<RoutingEntry> toQuery = new TreeSet<>((a, b) -> compareKeys(a, b, keyId));

        List<PeerAddresses> localClosest = engine.getKClosestPeers(key);

        if (maxCount == 1) {
            Collection<Multiaddr> existing = addressBook.get(PeerId.fromBase58(peerIdkey.toBase58())).join();
            if (!existing.isEmpty())
                return Collections.singletonList(new PeerAddresses(peerIdkey, existing.stream().map(a -> a.toString()).map(MultiAddress::new).collect(Collectors.toList())));
            Optional<PeerAddresses> match = localClosest.stream().filter(p -> p.peerId.equals(peerIdkey)).findFirst();
            if (match.isPresent())
                return Collections.singletonList(match.get());
        }

        // At initialization toQuery is seeded with the k peers from our routing table we know are closest to Key, based on the XOR distance function
        closest.addAll(localClosest.stream()
                .map(p -> new RoutingEntry(Id.create(Hash.sha256(p.peerId.toBytes()), 256), p))
                .collect(Collectors.toList()));
        toQuery.addAll(closest);

        // We keep track of the set of peers we've already queried
        Set<Multihash> queried = new HashSet<>();

        while (true) {
            // The set of next query candidates. Pick as many peers from the candidate peers (closest) as the alpha concurrency factor allows.
            List<RoutingEntry> queryThisRound = toQuery.stream().limit(alpha).collect(Collectors.toList());
            toQuery.removeAll(queryThisRound);

            // Send each a FIND_NODE(Key) request, and mark it as queried in Pq.
            queryThisRound.forEach(r -> queried.add(r.addresses().peerId));
            List<CompletableFuture<List<PeerAddresses>>> futures = queryThisRound.stream()
                    .map(r -> getCloserPeers(peerIdkey, r.addresses(), us))
                    .collect(Collectors.toList());

            boolean foundCloser = false;
            for (CompletableFuture<List<PeerAddresses>> future : futures) {
                // If successful the response will contain the k closest nodes the peer knows to the key
                List<PeerAddresses> result = future.join();
                for (PeerAddresses peer : result) {
                    if (!queried.contains(peer.peerId)) {
                        // exit early if we are looking for the specific node
                        if (maxCount == 1 && peer.peerId.equals(peerIdkey))
                            return Collections.singletonList(peer);
                        queried.add(peer.peerId);
                        Id peerKey = Id.create(Hash.sha256(peer.peerId.toBytes()), 256);
                        RoutingEntry e = new RoutingEntry(peerKey, peer);
                        toQuery.add(e);
                        closest.add(e);
                        foundCloser = true;
                    }
                }
            }

            // if no new peers in top k were returned we are done
            if (!foundCloser)
                break;
        }
        return closest.stream()
                .limit(maxCount).map(r -> r.addresses())
                .collect(Collectors.toList());
    }

    public CompletableFuture<List<PeerAddresses>> findProviders(Multihash block, Host us, int desiredCount) {
        byte[] key = block.bareMultihash().toBytes();
        Id keyId = Id.create(key, 256);
        List<PeerAddresses> providers = new ArrayList<>();

        SortedSet<RoutingEntry> toQuery = new TreeSet<>((a, b) -> b.key().getSharedPrefixLength(keyId) - a.key().getSharedPrefixLength(keyId));
        toQuery.addAll(engine.getKClosestPeers(key).stream()
                .map(p -> new RoutingEntry(Id.create(Hash.sha256(p.peerId.toBytes()), 256), p))
                .collect(Collectors.toList()));

        Set<Multihash> queried = new HashSet<>();

        while (true) {
            if (providers.size() >= desiredCount)
                return CompletableFuture.completedFuture(providers);
            List<RoutingEntry> queryThisRound = toQuery.stream().limit(alpha).collect(Collectors.toList());
            toQuery.removeAll(queryThisRound);
            queryThisRound.forEach(r -> queried.add(r.addresses().peerId));
            List<CompletableFuture<Providers>> futures = queryThisRound.stream()
                    .parallel()
                    .map(r -> dialPeer(r.addresses(), us).join().getProviders(block).orTimeout(2, TimeUnit.SECONDS))
                    .collect(Collectors.toList());
            boolean foundCloser = false;
            for (CompletableFuture<Providers> future : futures) {
                try {
                    Providers newProviders = future.join();
                    providers.addAll(newProviders.providers);
                    for (PeerAddresses peer : newProviders.closerPeers) {
                        if (!queried.contains(peer.peerId)) {
                            queried.add(peer.peerId);
                            RoutingEntry e = new RoutingEntry(Id.create(Hash.sha256(peer.peerId.toBytes()), 256), peer);
                            toQuery.add(e);
                            foundCloser = true;
                        }
                    }
                } catch (Exception e) {
                    if (!(e.getCause() instanceof TimeoutException))
                        e.printStackTrace();
                }
            }
            // if no new peers in top k were returned we are done
            if (!foundCloser)
                break;
        }

        return CompletableFuture.completedFuture(providers);
    }

    private CompletableFuture<List<PeerAddresses>> getCloserPeers(Multihash peerIDKey, PeerAddresses target, Host us) {
        try {
            return dialPeer(target, us).orTimeout(2, TimeUnit.SECONDS).join().closerPeers(peerIDKey);
        } catch (Exception e) {
            if (e.getCause() instanceof NothingToCompleteException)
                LOG.info("Couldn't dial " + peerIDKey + " addrs: " + target.addresses);
            else if (e.getCause() instanceof TimeoutException)
                LOG.info("Timeout dialing " + peerIDKey + " addrs: " + target.addresses);
            else
                e.printStackTrace();
        }
        return CompletableFuture.completedFuture(Collections.emptyList());
    }

    private Multiaddr[] getPublic(PeerAddresses target) {
        return target.addresses.stream()
                .filter(a -> localDht || a.isPublic(false))
                .map(a -> Multiaddr.fromString(a.toString()))
                .collect(Collectors.toList()).toArray(new Multiaddr[0]);
    }

    private CompletableFuture<? extends KademliaController> dialPeer(PeerAddresses target, Host us) {
        Multiaddr[] multiaddrs = getPublic(target);
        return dial(us, PeerId.fromBase58(target.peerId.toBase58()), multiaddrs).getController();
    }

    public CompletableFuture<Void> provideBlock(Multihash block, Host us, PeerAddresses ourAddrs) {
        List<PeerAddresses> closestPeers = findClosestPeers(block, replication, us);
        List<CompletableFuture<Boolean>> provides = closestPeers.stream()
                .parallel()
                .map(p -> dialPeer(p, us).join().provide(block, ourAddrs))
                .collect(Collectors.toList());
        return CompletableFuture.allOf(provides.toArray(new CompletableFuture[0]));
    }

    public CompletableFuture<Void> publishIpnsValue(PrivKey priv, Multihash publisher, Multihash value, long sequence, Host us) {
        int hours = 1;
        LocalDateTime expiry = LocalDateTime.now().plusHours(hours);
        long ttl = hours * 3600_000_000_000L;

        int publishes = 0;
        while (publishes < replication) {
            List<PeerAddresses> closestPeers = findClosestPeers(publisher, replication, us);
            for (PeerAddresses peer : closestPeers) {
                boolean success = dialPeer(peer, us).join().putValue("/ipfs/" + value, expiry, sequence,
                        ttl, publisher, priv).join();
                if (success)
                    publishes++;
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    public CompletableFuture<String> resolveIpnsValue(Multihash publisher, Host us) {
        List<PeerAddresses> closestPeers = findClosestPeers(publisher, replication, us);
        List<IpnsRecord> candidates = new ArrayList<>();
        Set<PeerAddresses> queryCandidates = new HashSet<>();
        Set<Multihash> queriedPeers = new HashSet<>();
        for (PeerAddresses peer : closestPeers) {
            if (queriedPeers.contains(peer.peerId))
                continue;
            queriedPeers.add(peer.peerId);
            GetResult res = dialPeer(peer, us).join().getValue(publisher).join();
            if (res.record.isPresent() && res.record.get().publisher.equals(publisher))
                candidates.add(res.record.get().value);
            queryCandidates.addAll(res.closerPeers);
        }

        // Validate and sort records by sequence number
        List<IpnsRecord> records = candidates.stream().sorted().collect(Collectors.toList());
        return CompletableFuture.completedFuture(records.get(records.size() - 1).value);
    }
}
