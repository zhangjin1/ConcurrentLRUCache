package com.weimob.mengdian.supplier.util;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @see jodd.LRUCache
 * 
 * @author zhangjin
 *
 * @param <String>
 * @param <GoodsCacheDTO>
 */
public class LRUCache<K, V> extends LinkedHashMap<K, V> {
    private static final long serialVersionUID = 8530000893524827816L;
    private int cacheSize;
    private ReadWriteLock lock = new ReentrantReadWriteLock();

    public LRUCache(int cacheSize) {
        super(cacheSize, 0.75f, false);// 先进先出的策略
        this.cacheSize = cacheSize;
    }

    public void saveGoods(K k, V v) {
        try {
            lock.writeLock().lock();
            put(k, v);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public V getGoods(K key) {
        try {
            lock.readLock().lock();
            return get(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    public void deleteGoods(K key) {
        try {
            lock.writeLock().lock();
            remove(key);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return size() >= cacheSize;
    }

}
