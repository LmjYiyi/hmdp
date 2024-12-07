package com.hmdp.utils;

public interface Lock {
    boolean tryLock(long timeoutSec);
    void unlock();
}
