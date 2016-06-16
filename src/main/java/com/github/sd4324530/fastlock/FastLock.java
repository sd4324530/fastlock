package com.github.sd4324530.fastlock;

import java.util.concurrent.locks.Lock;

/**
 * 锁接口，支持超时，有效防止死锁
 * @author peiyu
 */
public interface FastLock extends Lock {

    /**
     * 获取超时时间
     * @return 超时时间
     */
    Long getLockTimeout();
}
