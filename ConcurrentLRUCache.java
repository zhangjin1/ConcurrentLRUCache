package com.weimob.mengdian.supplier.util;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import lombok.extern.slf4j.Slf4j;

/**
 * 无论是热点数据还是LRU只需定义好CacheItem的count就行,类似的方案有guava的CacheLoader
 * 
 * @author zhangjin
 *
 * @param <K>
 * @param <V>
 */
@Slf4j
public class ConcurrentLRUCache<K, V> {

    private int maxCacheSize;// 最大缓存数量
    private Map<K, CacheItem<V>> cache = new ConcurrentHashMap<>();
    private AccessHolder accessHolder = new AccessHolder();
    private AtomicLong evictCount = new AtomicLong();// 驱逐缓存数量
    private AtomicLong hitCount = new AtomicLong();// 命中
    private AtomicLong notHitCount = new AtomicLong();// 未命中
    private AtomicInteger exceedCount = new AtomicInteger();// 超出最大缓存的数量
    private Lock lock = new ReentrantLock();
    private DecimalFormat decimalFormat = new DecimalFormat(".00");

    public ConcurrentLRUCache(int maxCacheSize) {
        cache = new ConcurrentHashMap<>(maxCacheSize, 1, 50);
        this.maxCacheSize = maxCacheSize;
    }

    public void clear() {
        cache.clear();
        evictCount.set(0);
        hitCount.set(0);
        notHitCount.set(0);
        accessHolder.clear();
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
        status.append("最大缓存数量: ").append(maxCacheSize).append(";").append("实际缓存数量: ").append(cache.size()).append(";")
                .append("驱逐缓存次数: ").append(evictCount.get()).append(";").append("命中缓存次数: ").append(hitCount.get())
                .append(";").append("未命中缓存次数: ").append(notHitCount.get()).append(";").append("缓存命中比例: ")
                .append(total == 0 ? 0 : decimalFormat.format(hitCount.get() / (float) total * 100)).append("%;");
        return status.toString();
    }

    /**
     * 并发情况下,如果位置2还未执行而位置1先被判断就会造成超出maxCacheSize
     */
    public void put(K key, V value) {
        // #1
        if (getCacheSize() >= maxCacheSize) {
            exceedCount.incrementAndGet();
            lock.lock();
            try {
                int cacheSize = getCacheSize();
                if (cacheSize >= maxCacheSize) {
                    int count = exceedCount.get() + (cacheSize - maxCacheSize);
                    if (count >= cacheSize) {
                        count = cacheSize;
                    }
                    accessHolder.removeLastAccessor(count);
                    evictCount.addAndGet(count);
                    exceedCount.addAndGet(-count);
                }
            } catch (Exception e) {
                log.error("exceed elements error!detail:", e);
            } finally {
                lock.unlock();
            }
        }
        // #2
        CacheItem<V> item = new CacheItem<V>(key, value);
        // log.info("new item key is {}", key);
        if (cache.put(key, item) == null)
            accessHolder.accessItem(item);
        else {
            // log.info("{} is already loaded and only put queue once !!!",
            // key);
        }
    }

    private int getCacheSize() {
        return cache.size();
    }

    public V get(K key) {
        CacheItem<V> item = cache.get(key);
        if (item != null) {
            // log.info("{} hit!!!", key);
            accessHolder.accessItem(item);
            hitCount.incrementAndGet();
            return item.getValue();
        }
        notHitCount.incrementAndGet();
        return null;
    }

    public void remove(K key) {
        CacheItem<V> item = cache.remove(key);
        if (item != null)
            accessHolder.removeAccessor(item);
    }

    public List<V> getAllElements() {
        List<V> values = new ArrayList<V>();
        Set<Entry<K, CacheItem<V>>> items = cache.entrySet();
        for (Entry<K, CacheItem<V>> entry : items) {
            values.add(entry.getValue().getValue());
        }
        return values;
    }

    private class CacheItem<T> {
        private T value;
        private K key;
        private CacheItem<T> next;
        private CacheItem<T> prev;

        public CacheItem(K key, T value) {
            this.value = value;
            this.key = key;
        }

        public T getValue() {
            return value;
        }

        public K getKey() {
            return key;
        }

        @Override
        public boolean equals(Object other) {
            if (other instanceof CacheItem) {
                @SuppressWarnings("unchecked")
                CacheItem<V> item = (CacheItem<V>) other;
                return value == item.getValue();
            }
            return false;
        }

        public boolean isNewAccessor() {
            return next == null && prev == null;
        }
    }

    private class AccessHolder {
        AccessorLinkedList list = new AccessorLinkedList();
        LinkedBlockingQueue<CacheItem<V>> queue = new LinkedBlockingQueue<CacheItem<V>>(1000);

        {
            Thread accessorProcess = new Thread(new QueueHandler(), "accessorProcess");
            accessorProcess.start();
        }

        public void clear() {
            list.clear();
            queue.clear();
        }

        public void removeLastAccessor(int count) {
            // log.info("origin list is {} and evict {} accessor!!!",
            // list.toString(), count);
            if (count == getCacheSize()) {
                clear();
                cache.clear();
            }
            CacheItem<V> lastOne;
            int succ = 0;
            while (succ < count) {
                lastOne = list.unlinkLast();
                // log.info("last is {}", lastOne.getKey());
                K lastKey;
                if ((lastKey = lastOne.getKey()) != null && cache.remove(lastKey) != null) {
                    succ++;
                }
            }
            // log.info(list.toString());
        }

        public void accessItem(CacheItem<V> item) {
            try {
                // log.info("{} put queue!!!", item.getKey());
                queue.offer(item);// 以非阻塞的方式隔离处理queue的线程挂掉对主线程带来的影响
            } catch (Exception e) {
                log.error("put element error for queue! item:{}", item, e);
            }
        }

        public void removeAccessor(CacheItem<V> item) {
            // log.info("unlink {}", item.getKey());
            queue.remove(item);
            list.unlink(item);
            // log.info(queue.toString());
            // log.info(list.toString());
        }

        private class QueueHandler implements Runnable {
            @Override
            public void run() {
                log.info("@@@{} have started...........", Thread.currentThread().getName());
                CacheItem<V> item = null;
                for (;;) {
                    try {
                        item = queue.take();
                        // log.info("get one from queue is {}", item.getKey());
                        list.addAccessor(item);
                    } catch (Exception e) {
                        log.error("get element error from queue! item:{}", item, e);
                    }
                }
            }
        }

    }

    private class AccessorLinkedList {
        private Lock lock = new ReentrantLock();
        private CacheItem<V> first;
        private CacheItem<V> last;

        public AccessorLinkedList() {
            init();
        }

        private void init() {
            CacheItem<V> item = new CacheItem<V>(null, null);
            first = last = item;
        }

        /**
         * 新加入的节点连接到链尾1/5处，已经在的节点链接到first节点，优化批量查询导致的效率低下
         */
        public void addAccessor(CacheItem<V> item) {
            lock.lock();
            try {
                if (item.isNewAccessor()) {
                    // log.info("{} is new accessor", item.getKey());
                    int index = (cache.size() - 1) / 5;
                    CacheItem<V> it = last;
                    for (int i = 0; i < index && it != null; i++) {
                        it = it.prev;
                    }
                    if (it == null)
                        linkLast(item);
                    else
                        linkBefore(item, it);
                    // log.info(toString());
                } else {
                    // log.info("{} has been access and linekd to first!!!",
                    // item.getKey());
                    unlink(item);
                    linkBefore(item, first);
                    // log.info(toString());
                }
            } finally {
                lock.unlock();
            }
        }

        @SuppressWarnings("unused")
        private void linkFirst(CacheItem<V> newNode) {
            CacheItem<V> f = first;
            newNode.next = f;
            first = newNode;
            if (f == null)
                last = newNode;
            else
                f.prev = newNode;
        }

        private void linkLast(CacheItem<V> newNode) {
            final CacheItem<V> l = last;
            newNode.prev = l;
            last = newNode;
            if (l == null)
                first = newNode;
            else
                l.next = newNode;
        }

        @Override
        public String toString() {
            lock.lock();
            try {
                StringBuilder builder = new StringBuilder();
                CacheItem<V> it = last;
                while (it != null) {
                    builder.append(it.getKey()).append("==>");
                    it = it.prev;
                }
                return builder.toString();
            } finally {
                lock.unlock();
            }
        }

        public void clear() {
            lock.lock();
            try {
                for (CacheItem<V> x = first; x != null;) {
                    CacheItem<V> next = x.next;
                    x.next = null;
                    x.prev = null;
                    x = next;
                }
                first = last = null;
            } finally {
                init();
                lock.unlock();
            }
        }

        private CacheItem<V> unlinkLast() {
            lock.lock();
            try {
                CacheItem<V> prev = last.prev;
                CacheItem<V> lastOne = last;
                last.prev = null; // help GC
                last = prev;
                if (prev == null)
                    first = null;
                else
                    prev.next = null;
                return lastOne;
            } finally {
                lock.unlock();
            }
        }

        public void linkBefore(CacheItem<V> item, CacheItem<V> next) {
            lock.lock();
            try {
                CacheItem<V> pred = next.prev;
                item.prev = pred;
                item.next = next;
                next.prev = item;
                if (pred == null)
                    first = item;
                else
                    pred.next = item;
            } finally {
                lock.unlock();
            }
        }

        private void unlink(CacheItem<V> item) {
            lock.lock();
            try {
                final CacheItem<V> next = item.next;
                final CacheItem<V> prev = item.prev;
                if (prev == null) {
                    first = next;
                } else {
                    prev.next = next;
                    item.prev = null;
                }
                if (next == null) {
                    last = prev;
                } else {
                    next.prev = prev;
                    item.next = null;
                }
            } finally {
                lock.unlock();
            }
        }

    }
}
