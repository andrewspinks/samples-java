package io.temporal.samples.claimcheckmemo;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Stand-in for an external object store (S3, GCS, ...) used by the claim-check pattern.
 *
 * <p>In a real deployment {@link #put(byte[])} would upload bytes and return a reference (URL /
 * key) and {@link #get(String)} would download them. Here it is an in-process map so the sample is
 * self-contained. The {@code putCount} / {@code fetchCount} counters let the sample prove how many
 * object-store round trips each scenario causes.
 */
final class ClaimCheckObjectStore {

  private static final ConcurrentHashMap<String, byte[]> STORE = new ConcurrentHashMap<>();
  private static final AtomicInteger PUT_COUNT = new AtomicInteger();
  private static final AtomicInteger FETCH_COUNT = new AtomicInteger();

  /** Offload bytes, returning the reference key that is left inline in Temporal. */
  static String put(byte[] data) {
    String key = UUID.randomUUID().toString();
    STORE.put(key, data);
    PUT_COUNT.incrementAndGet();
    return key;
  }

  /** Fetch previously offloaded bytes. Every call is a (simulated) object-store round trip. */
  static byte[] get(String key) {
    System.out.println("Queried store for " + key);
    FETCH_COUNT.incrementAndGet();
    byte[] data = STORE.get(key);
    if (data == null) {
      throw new IllegalStateException("No object found for claim-check key " + key);
    }
    return data;
  }

  /** Reset everything between scenarios. */
  static void resetAll() {
    STORE.clear();
    PUT_COUNT.set(0);
    FETCH_COUNT.set(0);
  }

  private ClaimCheckObjectStore() {}
}
