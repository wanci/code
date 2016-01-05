# coding=utf8
"""
使用redis 的setnx, getset原子操作来实现一个分布式锁
by leohli
"""

import redis
import time

# 锁超时时间
LOCK_TIME_OUT = 5
LOCK = 'monitor_lock'
r = redis.Redis()

"""
获取锁
"""
def getLock():
    now = int(time.time())    #获取当前时间戳
    timestamp = now + LOCK_TIME_OUT + 1
    result = r.setnx(LOCK, timestamp)
    if result:          #直接加锁成功
        return True
    else:
        expireTime = int(r.get(LOCK))
        if now > expireTime and now > int(r.getset(LOCK, timestamp)):
            return True
        else:
            print "Acquire Lock Failed!"
            return False

"""
释放锁
"""
def releaseLock():
    now = int(time.time())
    if now < r.get(LOCK):
        r.delete(LOCK)

"""
任务 woker
"""
def worker():
    print "Do My Work!"
    time.sleep(2)
    print "sleep 60s"


"""
主轮询
"""
while True:
    if getLock():
        worker()
        releaseLock()
        time.sleep(1)
    else:
        time.sleep(1)