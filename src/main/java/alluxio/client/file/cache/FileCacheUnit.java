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

import alluxio.client.file.cache.struct.DoubleLinkedList;
import javafx.util.Pair;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class FileCacheUnit {
  private DoubleLinkedList<CacheInternalUnit> cacheList;
  private long mFileId;
  private long mLength;
  private DoubleLinkedList<CacheInternalUnit>  mFreeList = new DoubleLinkedList<>(new CacheInternalUnit(0,0,-1));
  public LinkedFileBucket mBuckets;
  private static boolean use_bucket;
  private final ClientCacheContext.LockManager mLockManager;

  static {
    use_bucket = ClientCacheContext.INSTANCE.USE_INDEX_0;
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

  public void cacheCoinFiliter(PriorityQueue<Pair<Long, Long>> queue,
                               Queue<Pair<Long, Long>> tmpQueue ){
    long maxEnd = -1;
    long minBegin = -1;
    while(!queue.isEmpty()) {
      Pair<Long, Long> tmpUnit = queue.poll();

      if(minBegin == -1) {
        minBegin = tmpUnit.getKey();
        maxEnd = tmpUnit.getValue();
      } else {
        if(tmpUnit.getKey() <= maxEnd) {
          maxEnd = Math.max(tmpUnit.getValue() , maxEnd);
        }
        else {
          tmpQueue.add(new Pair<>(minBegin, maxEnd));
          minBegin = tmpUnit.getKey();
          maxEnd = tmpUnit.getValue();
        }
      }
    }
    tmpQueue.add(new Pair<>(minBegin, maxEnd));
  }

  public long elimiate(Set<CacheUnit> input) {
  	long deleteSizeSum = 0;
		HashMap<CacheUnit, PriorityQueue<Pair<Long, Long>>> tmpMap = new HashMap<>();
    for(CacheUnit unit : input) {
			CacheInternalUnit cache = (CacheInternalUnit)getKeyFromBucket(unit.getBegin(), unit.getEnd());
		  cache.readLock.readLock().unlock();

			if(!tmpMap.containsKey(cache)) {
        PriorityQueue<Pair<Long, Long>> queue = new PriorityQueue<>(new Comparator<Pair<Long, Long>>() {
          @Override
          public int compare(Pair<Long, Long> o1, Pair<Long, Long> o2) {
            return (int)(o1.getKey() - o2.getKey());
          }
        });
        queue.add(new Pair<>(unit.getBegin(), unit.getEnd()));
        tmpMap.put(cache, queue);
      }
      else {
        tmpMap.get(cache).add(new Pair<>(unit.getBegin(), unit.getEnd()));
      }
		}
		CacheInternalUnit current = cacheList.head.after;
    Queue<Pair<Long, Long>> tmpQueue = new LinkedBlockingQueue<>();
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
        }
        if(!deleteAll) {
					deleteSize = current.getDeleteSize();
				} else {
        	deleteSize = current.getSize();
					if(ClientCacheContext.INSTANCE.mUseGhostCache ) {
						ClientCacheContext.INSTANCE.getGhostCache().add(new BaseCacheUnit(current.getBegin(),
							current.getEnd(), mFileId));
					}
				}
				next = current.after;
				if (deleteSize > 0) {
          mBuckets.delete(current);
          deleteSizeSum += deleteSize;
					if (next != null) {
						cacheList.delete(current);
					} else {
						cacheList.delete(cacheList.tail);
					}
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
    return deleteSizeSum;
  }

  public CacheInternalUnit addCache(TempCacheUnit unit) {
    // delete cache unit index in bucket
    if(use_bucket && unit.hasResource()) {
      for(CacheInternalUnit unit1 : unit.mCacheConsumer) {
        mBuckets.delete(unit1);
      }
    }

    CacheInternalUnit result = unit.convert();
    // TODO if needed delete the useless unit in list, but the data are in new
    // unit. so it's unsure to delete pointer or not.
    /*
    CacheUnit begin = unit.mBefore == null ? null : unit.mBefore.after;
    CacheUnit end = unit.mEnd == null? null : unit.mAfter.before;
    mFreeList.addSubLists(unit.mBefore.after, unit.mAfter.before);
    */
   // System.out.println((unit.mBefore== null ? "null" : unit.mBefore.toString()) + " || " + (unit.mAfter == null ? "null" : unit.mAfter.toString()));
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

  /**
   * Free unUsed cacheInternalUnit, delete data if bytebuf ref = 0
   */
  public void freeCache() {
    Iterator<CacheInternalUnit> iter = mFreeList.iterator();
    CacheInternalUnit pre = null;
    while(iter.hasNext()) {
      CacheInternalUnit unit = iter.next();
      if(pre != null) {
        if(pre.clearData()) {
          if(pre.before != null) {
            pre.before.after = pre.after;
          }
          if(pre.after != null) {
            pre.after.before = pre.before;
          }
          pre = null;
        }
      }
      pre = unit;
    }
  }
}
