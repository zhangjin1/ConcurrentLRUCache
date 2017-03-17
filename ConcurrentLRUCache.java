package com.weimob.mengdian.supplier.util;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 无论是热点数据还是LRU只需定义好CacheItem的count就行
 * 
 * @author zhangjin
 *
 * @param <K>
 * @param <V>
 */
// @Slf4j
public class ConcurrentLRUCache<K, V> {

    private int maxCacheSize;// 最大缓存数量
    private Map<K, CacheItem<V>> cache = new ConcurrentHashMap<>();
    private AtomicLong evictCount = new AtomicLong();// 驱逐缓存数量
    private AtomicLong hitCount = new AtomicLong();// 命中
    private AtomicLong notHitCount = new AtomicLong();// 未命中
    private AtomicInteger exceedCount = new AtomicInteger();// 超出最大缓存的数量
    private Lock lock = new ReentrantLock();

    // private ThreadLocal<StringBuilder> threadLocal = new
    // ThreadLocal<StringBuilder>();

    public ConcurrentLRUCache(int maxCacheSize) {
        cache = new ConcurrentHashMap<>(maxCacheSize, 1, 50);
        this.maxCacheSize = maxCacheSize;
    }

    public void clear() {
        cache.clear();
        evictCount.set(0);
        hitCount.set(0);
        notHitCount.set(0);
    }

    public void changeMaxSize(int newSize) {
        this.maxCacheSize = newSize;
    }

    public Set<K> getKeys() {
        return cache.keySet();
    }

    public String getStatus() {
        StringBuilder status = new StringBuilder();
        long total = hitCount.get() + notHitCount.get();
        status.append("最大缓存数量: ").append(maxCacheSize).append(";").append("当前缓存数量: ").append(cache.size()).append(";")
                .append("驱逐缓存次数: ").append(evictCount.get()).append(";").append("命中缓存次数: ").append(hitCount.get())
                .append(";").append("未命中缓存次数: ").append(notHitCount.get()).append(";").append("缓存命中比例: ")
                .append(total == 0 ? 0 : hitCount.get() / (float) total * 100).append(" %;");
        return status.toString();
    }

    public void put(K key, V value) {
        if (cache.size() >= maxCacheSize) {
            exceedCount.incrementAndGet();
            // threadLocal.get().append("exceed count and wait lock!");
            lock.lock();
            try {
                int cacheSize = cache.size();
                // threadLocal.get().append("get lock and cs>ms:" + (cacheSize
                // >= maxCacheSize) + "!");
                if (cacheSize >= maxCacheSize) {
                    // try {
                    // Thread.sleep(1000);
                    // } catch (InterruptedException e) {
                    // }
                    int count = exceedCount.get();
                    if (count >= cacheSize) {
                        count = cacheSize;
                    }
                    // threadLocal.get().append("exceedCount: " + count + ";");
                    removeLast(count);
                    evictCount.addAndGet(count);
                    exceedCount.addAndGet(-count);
                }
            } finally {
                lock.unlock();
            }
        }
        CacheItem<V> item = new CacheItem<V>(value, new AtomicInteger(1));
        cache.put(key, item);
        // log.info("@@@{}###{}", Thread.currentThread().getName(),
        // threadLocal.get().toString());
    }

    private void removeLast(int count) {// 移除最后count个,参照guava的Ordering类
        if (count == cache.size()) {
            cache.clear();
        }
        @SuppressWarnings("unchecked")
        Map.Entry<K, CacheItem<V>>[] buffer = new Map.Entry[count];
        Set<Map.Entry<K, CacheItem<V>>> elements = cache.entrySet();
        Iterator<Map.Entry<K, CacheItem<V>>> iterator = elements.iterator();
        int bufferSize = 0;
        Map.Entry<K, CacheItem<V>> item;
        // StringBuilder str = new StringBuilder();
        while (bufferSize < count && iterator.hasNext()) {
            item = iterator.next();
            buffer[bufferSize++] = item;
            // str.append(item.getValue().getCount() + ";");
        }
        while (iterator.hasNext()) {
            item = iterator.next();
            // str.append(item.getValue().getCount() + ";");
            for (int i = 0; i < buffer.length && buffer[i] != null; i++) {
                if (item.getValue().compareTo(buffer[i].getValue()) < 0) {
                    buffer[i] = item;
                    break;
                }
            }
        }
        for (int i = 0; i < buffer.length && buffer[i] != null; i++) {
            cache.remove(buffer[i].getKey());
            // threadLocal.get().append(
            // "evict: " + buffer[i].getKey() + ": " +
            // buffer[i].getValue().getCount() + ";allCount: "
            // + str.toString() + ";");
        }
    }

    public V get(K key) {
        // threadLocal.set(new StringBuilder());
        CacheItem<V> item = cache.get(key);
        if (item != null) {
            item.hit();
            // threadLocal.get().append("hit : " + key + ";");
            // log.info("@@@{}###{}", Thread.currentThread().getName(),
            // threadLocal.get().toString());
            hitCount.incrementAndGet();
            return item.getValue();
        }
        // threadLocal.get().append("not hit: " + key + ";");
        notHitCount.incrementAndGet();
        return null;
    }

    public void remove(K key) {
        cache.remove(key);
    }

    private static class CacheItem<V> {
        private V value;
        private AtomicInteger count;

        public CacheItem(V value, AtomicInteger count) {
            this.value = value;
            this.count = count;
        }

        public void hit() {
            this.count.incrementAndGet();
        }

        public V getValue() {
            return value;
        }

        public int getCount() {
            return count.get();
        }

        public int compareTo(CacheItem<V> c) {
            if (getCount() > c.getCount()) {
                return 1;
            } else if (getCount() < c.getCount()) {
                return -1;
            }
            return 0;
        }
    }
}
