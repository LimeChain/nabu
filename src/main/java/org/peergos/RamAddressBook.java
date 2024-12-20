package org.peergos;

import io.libp2p.core.AddressBook;
import io.libp2p.core.PeerId;
import io.libp2p.core.multiformats.Multiaddr;
import io.libp2p.core.multiformats.MultiaddrComponent;
import io.libp2p.core.multiformats.Protocol;
import org.jetbrains.annotations.NotNull;
import org.peergos.util.LRUCache;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RamAddressBook implements AddressBook {

    private final Map<PeerId, Set<Multiaddr>> addresses;

    public RamAddressBook() {
        addresses = Collections.synchronizedMap(new LRUCache<>(10_000));
    }

    @NotNull
    @Override
    public CompletableFuture<Void> addAddrs(@NotNull PeerId peerId, long ttl, @NotNull Multiaddr... multiaddrs) {
        addresses.putIfAbsent(peerId, new HashSet<>());
        Set<Multiaddr> val = addresses.get(peerId);
        synchronized (val) {
            val.addAll(withoutPeerId(multiaddrs, peerId));
        }
        return CompletableFuture.completedFuture(null);
    }

    @NotNull
    @Override
    public CompletableFuture<Collection<Multiaddr>> getAddrs(@NotNull PeerId peerId) {
        return CompletableFuture.completedFuture(new ArrayList(addresses.getOrDefault(peerId, Collections.emptySet())));
    }

    @NotNull
    @Override
    public CompletableFuture<Void> setAddrs(@NotNull PeerId peerId, long ttl, @NotNull Multiaddr... multiaddrs) {
        Set<Multiaddr> val = new HashSet<>();
        val.addAll(withoutPeerId(multiaddrs, peerId));
        addresses.put(peerId, val);
        return CompletableFuture.completedFuture(null);
    }

    private static List<Multiaddr> withoutPeerId(Multiaddr[] in, PeerId id) {
        byte[] peerId = id.getBytes();
        return Stream.of(in)
                .map(a -> withoutPeerId(a, peerId))
                .collect(Collectors.toList());
    }

    private static Multiaddr withoutPeerId(Multiaddr in, byte[] peerId) {
        List<MultiaddrComponent> comp = in.getComponents();
        MultiaddrComponent last = comp.get(comp.size() - 1);
        if ((last.getProtocol() == Protocol.P2P || last.getProtocol() == Protocol.IPFS) &&
                Arrays.equals(last.getValue(), peerId))
            return new Multiaddr(comp.subList(0, comp.size() - 1));
        return in;
    }
}
