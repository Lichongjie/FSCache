/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.file.cache;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.cache.struct.DoubleLinkedList;
import alluxio.client.file.cache.struct.LongPair;
import alluxio.exception.AlluxioException;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class FileCacheUnit {
  private DoubleLinkedList<CacheInternalUnit> cacheList;
  private long mFileId;
  private long mLength;
  public LinkedFileBucket mBuckets;
  private static boolean use_bucket;
  private final ClientCacheContext.LockManager mLockManager;
  private static final ClientCacheContext mContext =  ClientCacheContext.INSTANCE;

  static {
    use_bucket =mContext.USE_INDEX_0;
  }

  public FileCacheUnit(long fileId, long length, ClientCacheContext.LockManager mLockManager) {
    mFileId = fileId;
    mLength = length;
    cacheList = new DoubleLinkedList<>(new CacheInternalUnit(0,0,-1));
    if(use_bucket) {
      mBuckets = new LinkedFileBucket(length, fileId, mLockManager);
    }
    this.mLockManager = mLockManager;
  }

  public DoubleLinkedList<CacheInternalUnit> getCacheList() {
    return cacheList;
  }

  public CacheUnit getKeyFromBucket(long begin, long end) {
		return mBuckets.find(begin, end);
  }

  public void cacheCoinFiliter(PriorityQueue<LongPair> queue,
                               Queue<LongPair> tmpQueue ){
    long maxEnd = -1;
    long minBegin = -1;
    while(!queue.isEmpty()) {
			LongPair tmpUnit = queue.poll();
      if(minBegin == -1) {
        minBegin = tmpUnit.getKey();
        maxEnd = tmpUnit.getValue();
      } else {
        if(tmpUnit.getKey() <= maxEnd) {
          maxEnd = Math.max(tmpUnit.getValue() , maxEnd);
        }
        else {
          tmpQueue.add(new LongPair(minBegin, maxEnd));
          minBegin = tmpUnit.getKey();
          maxEnd = tmpUnit.getValue();
        }
      }
    }
    tmpQueue.add(new LongPair(minBegin, maxEnd));
  }

  public void cacheCoinFiliter(Queue<CacheUnit> queue,
                               Queue<LongPair> tmpQueue ){
    long maxEnd = -1;
    long minBegin = -1;
    while(!queue.isEmpty()) {
      CacheUnit tmpUnit = queue.poll();
      if(minBegin == -1) {
        minBegin = tmpUnit.getBegin();
        maxEnd = tmpUnit.getEnd();
      } else {
        if(tmpUnit.getBegin() <= maxEnd) {
          maxEnd = Math.max(tmpUnit.getEnd() , maxEnd);
        }
        else {
          tmpQueue.add(new LongPair(minBegin, maxEnd));
          minBegin = tmpUnit.getBegin();
          maxEnd = tmpUnit.getEnd();
        }
      }
    }
    tmpQueue.add(new LongPair(minBegin, maxEnd));
  }


  /**
   * Return the new Cache size promoted from under_fs
   */
  public long merge(AlluxioURI uri, PriorityQueue<CacheUnit> queue)
          throws IOException, AlluxioException {
    Iterator<CacheInternalUnit> iterator = cacheList.iterator();
    FileSystem fs = FileSystem.Factory.get(true);
    FileInStream in = fs.openFile(uri);
    Set<CacheInternalUnit> needDeleteSet = new LinkedHashSet<>();
    while (iterator.hasNext()) {
      CacheInternalUnit tmp = iterator.next();
      queue.add(tmp);
      needDeleteSet.add(tmp);
    }
    Queue<LongPair> tmpQueue = new LinkedBlockingQueue<>();
    cacheCoinFiliter(queue, tmpQueue);
    long newSize = 0;
    while (!tmpQueue.isEmpty()) {
      LongPair l = tmpQueue.poll();
      CacheUnit newUnit = getKeyFromBucket(l.getKey(), l.getValue());
      if(newUnit instanceof TempCacheUnit) {
        TempCacheUnit tmpUnit = (TempCacheUnit)newUnit;
        for(CacheInternalUnit unit : tmpUnit.mCacheConsumer) {
          needDeleteSet.remove(unit);
        }
        tmpUnit.setInStream((FileInStreamWithCache)in);
        int len = (int)(l.getValue() - l.getKey());
        int res = mContext.getCacheManager().cache(tmpUnit, l.getKey(), len);
        if (res != len) {
          // the end of file
          tmpUnit.resetEnd((int) mLength);
        }
        newSize += res;
      }
    }
    for(CacheInternalUnit current : needDeleteSet) {
      List<ReentrantReadWriteLock> writeLocks = mLockManager.deleteLock(current);
      try {
        mBuckets.delete(current);
        cacheList.delete(current);
        current.clearData();
        newSize -= current.getSize();
      } finally {
        if( writeLocks != null) {
          for(ReentrantReadWriteLock w : writeLocks) {
            w.writeLock().unlock();
          }
        }
      }
    }
    return newSize;
  }


  public long elimiate(Set<CacheUnit> input) {
    long deleteSizeSum = 0;
    HashMap<CacheUnit, PriorityQueue<LongPair>> tmpMap = new HashMap<>();
    for(CacheUnit unit : input) {
      CacheInternalUnit cache = null;
      try {
        cache = (CacheInternalUnit) getKeyFromBucket(unit.getBegin(), unit.getEnd());
        mLockManager.readUnlock(cache.getFileId(), cache.readLock);
        if(!tmpMap.containsKey(cache)) {
          PriorityQueue<LongPair> queue = new PriorityQueue<>(new Comparator<LongPair>() {
            @Override
            public int compare(LongPair o1, LongPair o2) {
              return (int)(o1.getKey() - o2.getKey());
            }
          });
          LongPair p = new LongPair(unit.getBegin(), unit.getEnd());
          queue.add(p);
          tmpMap.put(cache, queue);
        } else {
          tmpMap.get(cache).add(new LongPair(unit.getBegin(), unit.getEnd()));
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    CacheInternalUnit current = cacheList.head.after;
    Queue<LongPair> tmpQueue = new LinkedBlockingQueue<>();
    while(current != null ){
      CacheInternalUnit next= null;
      boolean deleteAll = false;
      long deleteSize;
      List<ReentrantReadWriteLock> writeLocks = mLockManager.deleteLock(current);
      try {
        if (tmpMap.containsKey(current)) {
          cacheCoinFiliter(tmpMap.get(current), tmpQueue);
          current.split(tmpQueue, mBuckets);
          tmpQueue.clear();
        } else {
          deleteAll = true;
        } if(!deleteAll) {
          deleteSize = current.getDeleteSize();
        } else {
          deleteSize = current.getSize();
          if(mContext.mUseGhostCache ) {
            mContext.getGhostCache().add(new BaseCacheUnit(current.getBegin(),
              current.getEnd(), mFileId));
          }
        }
        next = current.after;
        if (deleteSize > 0) {
          mBuckets.delete(current);
          deleteSizeSum += deleteSize;
          cacheList.delete(current);
          current.clearData();
          current = null;
        }
      } finally {
        if( writeLocks != null) {
          for(ReentrantReadWriteLock w : writeLocks) {
            w.writeLock().unlock();
          }
        }
        current = next;
      }
    }
    tmpMap.clear();
    tmpMap = null;
    return deleteSizeSum;
  }

  public CacheInternalUnit addCache(TempCacheUnit unit) {
    CacheInternalUnit result = unit.convert();
    while(!unit.deleteQueue.isEmpty()) {
      CacheInternalUnit unit1 =unit.deleteQueue.poll();
      mBuckets.delete(unit1);
      unit1.clearData();
      unit1.before = unit1.after = null;
      unit1 = null;
    }

    cacheList.insertBetween(result, result.before,result.after);
    if(use_bucket) {
      mBuckets.add(result);
    }
    return result;
  }

  public void print() {
    System.out.println("cache list info : ");
    Iterator<CacheInternalUnit> i = cacheList.iterator();
    while(i.hasNext()) {
      CacheInternalUnit u = i.next();
      System.out.println(u.toString());
      u.printtest();
    }
    System.out.println();
    mBuckets.print();
  }
}
