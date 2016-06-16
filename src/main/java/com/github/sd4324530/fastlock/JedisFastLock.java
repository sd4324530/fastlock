package com.github.sd4324530.fastlock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCluster;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

/**
 * 基于redis集群实现的分布式锁
 * @author peiyu
 */
public class JedisFastLock implements FastLock {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final JedisCluster jedis;

    private final String lockKey;

    private static final ThreadLocal<Long> threadCache = new ThreadLocal<Long>();

    /**
     * 默认锁超时时间为1秒，防止出现死锁
     */
    private long lockTimeout = 2000;

    public JedisFastLock(JedisCluster jedis, String lockKey) {
        this.jedis = jedis;
        this.lockKey = lockKey;
    }

    @Override
    public void lock() {
        getLock();
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        getLock();
    }

    @Override
    public boolean tryLock() {
        try {
            getLock();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        long waitTime = unit.toMillis(time);
        long timeout = System.currentTimeMillis() + waitTime;
        do {
            long now = System.currentTimeMillis();
            if(now > timeout) {
                if(tryLock()) {
                    return true;
                }
            } else {
                return false;
            }
        } while (true);
    }

    @Override
    public void unlock() {
        //如果还没有过超时时间，则主动释放锁，如果已经超时，其他线程已经可以获取该锁了，无需主动释放
        Long lockTimeout = this.getLockTimeout();
        if(null != lockTimeout) {
            if(System.currentTimeMillis() < lockTimeout) {
                log.debug("开始释放锁....");
                threadCache.remove();
                Long del = this.jedis.del(this.lockKey);
                log.debug("主动释放锁成功:{}", del);
            }
        }
    }

    @Override
    public Condition newCondition() {
        throw new FastLockException("暂未支持");
    }

    @Override
    public Long getLockTimeout() {
        return threadCache.get();
    }

    void setTimeout(Long timeout) {
        this.lockTimeout = timeout;
    }


    private void getLock() {
        log.debug("开始获取锁....");
        long value = System.currentTimeMillis() + this.lockTimeout;
        Long setnx = this.jedis.setnx(this.lockKey, String.valueOf(value));
        //设置锁失败，锁已经存在
        if(0 == setnx) {
            String time = this.jedis.get(this.lockKey);
            if(null != time) {
                long timeout = Long.parseLong(time);
                long now = System.currentTimeMillis();
                //已经超时，可以获取锁
                if(timeout < now) {
                    long newTimeout = System.currentTimeMillis() + this.lockTimeout;
                    String time2 = this.jedis.getSet(this.lockKey, String.valueOf(newTimeout));
                    if(null != time2) {
                        long timeout2 = Long.parseLong(time2);
                        //说明锁被别人取走了，本次获取锁失败
                        if(timeout2 != timeout) {
                            throw new FastLockException("锁未被释放，无法获取锁！");
                        } else {
                            log.debug("获取锁成功1");
                            threadCache.set(newTimeout);
                        }
                    } else {
                        log.debug("获取锁成功2");
                        threadCache.set(newTimeout);
                    }
                }
                //无法获取锁
                else {
                    throw new FastLockException("锁未被释放，无法获取锁！");
                }
            }
            //锁已经被释放，可以获取锁
            else {
                long newTimeout = System.currentTimeMillis() + this.lockTimeout;
                String time2 = this.jedis.getSet(this.lockKey, String.valueOf(newTimeout));
                if(null != time2) {
                    throw new FastLockException("锁未被释放，无法获取锁！");
                } else {
                    log.debug("获取锁成功3");
                    threadCache.set(newTimeout);
                }
            }
        }
        //设置锁成功，同时获取锁
        else {
            log.debug("获取锁成功4");
            threadCache.set(value);
        }
    }
}
