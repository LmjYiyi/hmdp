package com.hmdp.utils;

import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;


public class RedisLock implements  Lock{

    private StringRedisTemplate stringRedisTemplate;
    private String name;
    private static final String KEY_PREFIX = "lock:";
    private static final String ID_PREFIX = UUID.randomUUID().toString()+"-";
    public RedisLock(StringRedisTemplate stringRedisTemplate,String name){
        this.stringRedisTemplate = stringRedisTemplate;
        this.name = name;
    }
    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;
    static {
        UNLOCK_SCRIPT = new DefaultRedisScript<>();
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("unlock.lua"));
        UNLOCK_SCRIPT.setResultType(Long.class);
    }
    @Override
    public boolean tryLock(long timeoutSec) {
         String threadId = ID_PREFIX+Thread.currentThread().getId()+"";
         Boolean res = stringRedisTemplate.opsForValue()
                 .setIfAbsent(KEY_PREFIX+name,threadId,timeoutSec, TimeUnit.SECONDS);
         return  Boolean.TRUE.equals(res);

    }

    @Override
    public void unlock() {
//        String currentThread = ID_PREFIX+Thread.currentThread().getId();
//        String reidsThread = stringRedisTemplate.opsForValue().get(KEY_PREFIX+name);
//        if(currentThread!=null||currentThread.equals(reidsThread))
//            stringRedisTemplate.delete(KEY_PREFIX+name);
        stringRedisTemplate.execute(UNLOCK_SCRIPT, Collections.singletonList(KEY_PREFIX+name),
                ID_PREFIX+Thread.currentThread().getId());


    }
}
