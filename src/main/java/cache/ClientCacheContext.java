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

package cache;

import alluxio.client.file.URIStatus;
import alluxio.client.file.cache.struct.DoubleLinkedList;
import alluxio.client.file.cache.struct.RBTree;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ClientCacheContext {
  public static final ClientCacheContext INSTANCE = create();
  public static long searchTime = 0 ;
  public static long insertTime = 0 ;
	public static long readTime = 0 ;
	public static final int CACHE_SIZE = 1024 * 1024;
  public static final int BUCKET_LENGTH = 10;
  public static final String mCacheSpaceLimit = "1G";
  public static final long mCacheLimit = getSpaceLimit();
  public static boolean REVERSE = true;
  public static boolean USE_INDEX_0 = true;
  public static final CacheManager CACHE_POLICY = new CacheManager();
	public static boolean mAllowCache = true;
	public static final LockManager mLockManager = LockManager.INSTANCE;

	public static boolean mUseGhostCache = false;

	public static GhostCache getGhostCache() {
		return GhostCache.INSTANCE;
	}

	public static long getSpaceLimit() {
    String num = mCacheSpaceLimit.substring(0, mCacheSpaceLimit.length() -1);
    char unit = mCacheSpaceLimit.charAt(mCacheSpaceLimit.length()-1);
    int n = Integer.parseInt(num);
    if(unit == 'M' || unit == 'm') {
      return n * 1024 *1024;
    }
    if(unit == 'K' || unit == 'k') {
      return n * 1024;
    }
    if(unit == 'G' || unit == 'g') {
      return n * 1024 * 1024 * 1024;
    }
    return n;
  }

  private static ClientCacheContext create() {
    return new ClientCacheContext();
  }

  public final ConcurrentHashMap<Long, FileCacheUnit> mFileIdToInternalList = new ConcurrentHashMap<>();

  public static long getSearchTime() {
    return searchTime;
  }

  public static long getInsertTime(){
    return insertTime;
  }

  private Iterator iter = null;

  @SuppressWarnings("unchecked")
  public CacheUnit getCache(URIStatus status, long begin, long end) {
    long beginTime = System.currentTimeMillis();
    try {
      long fileId = status.getFileId();
      FileCacheUnit unit = mFileIdToInternalList.get(fileId);
      if (unit == null) {
        unit = new FileCacheUnit(fileId, status.getLength(), mLockManager);
        mFileIdToInternalList.put(fileId, unit);
      }

      if(USE_INDEX_0) {
        return unit.getKeyFromBucket(begin, end);
      }

      if(!REVERSE) {
        return getKey2(begin, end, fileId);
      } else {
        return getKeyByReverse2(begin, end, fileId);
      }
    } finally {
      searchTime += (System.currentTimeMillis() - beginTime);
    }
  }

  /**
   * Return true if the unit is equal to one element in RBTree.
   */
  public CacheUnit getKeyByTree(long begin, long end, RBTree<CacheInternalUnit> tree, long fileId, int index) {
  	CacheInternalUnit x = tree.root;
    TempCacheUnit unit = new TempCacheUnit(begin, end, fileId);
		ReentrantReadWriteLock tmpLock = mLockManager.readLock(fileId, index);

		while (x != null)
    {
			if(begin == x.getBegin() && end == x.getEnd()) {
			  x.readLock = tmpLock;

				return x;
      } else if(begin >= x.getEnd()) {
				if(x.right != null) {
					x = x.right;
				} else {
					return handleUnCoincidence(unit, x, x.after, index);
				}
      } else if(end <= x.getBegin()) {
				if(x.left != null) {
					x = x.left;
				} else {
					return handleUnCoincidence(unit, x.before, x, index);
				}
      }
      else {
				if(unit.getEnd() > x.getEnd()) {
          unit = handleLeftCoincidence(x, unit, true, index);

        }
        if(unit.getBegin() < x.getBegin()) {
          unit = handleRightCoincidence(unit, x, true, index);

        }
        return unit;
      }
    }
    return unit;
  }

  //TODO
  public CacheUnit getKeyByIndex0(int begin, int end, long fileId) {
   // LinkedFileBucket fileBucket = mFileCacheIndex0.get(fileId);
    //Pair<CacheInternalUnit, CacheInternalUnit> res =  fileBucket.find(begin, end);
   // DoubleLinkedList<CacheInternalUnit> cacheList = mFileIdToInternalList.get(fileId)
   //     .getCacheList();
   // if(!REVERSE) {
   //   Iterator iter = cacheList.partitionItreatior(res.getKey(), res.getValue());
   //   return getKey(begin, end, fileId, iter);
  //  } else {
  ///    PreviousIterator iter = cacheList.partitionPreviousItreatior(res.getKey(), res.getValue());
  //   return getKeyByReverse(begin, end, fileId, iter);
  ///  }
    return null;
  }

  public CacheUnit getKeyByReverse(long begin, long end, long fileId, PreviousIterator iter, int bucketIndex) {
  	TempCacheUnit newUnit = new TempCacheUnit(begin, end, fileId);
    ReentrantReadWriteLock tmpLock = mLockManager.readLock(fileId, bucketIndex);
    CacheInternalUnit current = null;
    while(iter.hasPrevious()) {
      current = (CacheInternalUnit)iter.previous();
      long left = current.getBegin();
      long right = current.getEnd();
      //System.out.print(left + " " + right +"; ");
      if(end <= right) {
        if(begin >= left) {
					current.readLock = tmpLock;
					return current;
        }
        if (end < left) {
          CacheInternalUnit pre = current.before;
          if(pre == null || begin > pre.getEnd()) {
            return handleUnCoincidence(newUnit, current.before, current, bucketIndex);
          }
        }
        else {
          // right coincidence
          //TODO delete this judgement if allow (1,10)(10,20)=>(1,20)
          if(end != left)
            return handleRightCoincidence(newUnit, current, true, bucketIndex);
        }
      } else {
        //TODO change to > if allow (1,10)(10,20)=>(1,20)
        if(begin >= right) {
          return handleUnCoincidence(newUnit, current, current.after, bucketIndex);
        } else {
          return handleRightCoincidence(newUnit, current, true, bucketIndex);
        }
      }
    }
    return handleUnCoincidence(newUnit, null, current, bucketIndex);
  }

  /**
   * insert tempcacheunit to linkedlist
   *
   * @param unit the client read unit
   */
  public void insertSet(TempCacheUnit unit) {

  }

  public CacheUnit getKey(long begin, long end, long fileId, Iterator iter, int bucketIndex) {
    TempCacheUnit newUnit = new TempCacheUnit(begin, end, fileId);
    ReentrantReadWriteLock tmpLock = mLockManager.readLock(fileId, bucketIndex);

    CacheInternalUnit current = null;
    while(iter.hasNext()) {
      current = (CacheInternalUnit)iter.next();
      long left = current.getBegin();
      long right = current.getEnd();
      if(begin >= left) {
        if(end <= right) {
          current.readLock = tmpLock;
          return current;
        }
        if(begin > right) {
          CacheInternalUnit next = current.after;
          if(next == null || end < next.getBegin()) {
            return handleUnCoincidence(newUnit, current, current.after, bucketIndex);
            // return cacheList.insertAfert(newUnit, current);
          }
        } /*else if(begin == right){
          //TODO delete this, only for test
          CacheInternalUnit next = current.after;
          if(next == null)
          return handleUnCoincidence(newUnit, current, current.after);
        }*/
        else {
          //left Coincidence
          //TODO delete this judgement if allow (1,10)(10,20)=>(1,20)
          if(begin != right)
            return handleLeftCoincidence(current, newUnit, true, bucketIndex);
        }
      }
      else {
        if(end <= left) {
          return handleUnCoincidence(newUnit, current.before,current, bucketIndex);
          //return cacheList.insertBefore(newUnit, current);
        } else {
          //left unCoincidence
          return handleLeftCoincidence(current, newUnit, true, bucketIndex);
        }
      }
    }
    //return handleUnCoincidence(newUnit, cacheList.tail, null);
    return handleUnCoincidence(newUnit, current, null, bucketIndex);
  }

  public CacheUnit getKey2(long begin, long end, long fileId){
    DoubleLinkedList<CacheInternalUnit> cacheList = mFileIdToInternalList.get(fileId)
        .getCacheList();
    if(iter == null)
      iter = cacheList.iterator();
    return getKey(begin, end, fileId, iter, -1);
  }

  public CacheUnit getKeyFromBegin(long begin, long end, long fileId){
    DoubleLinkedList<CacheInternalUnit> cacheList = mFileIdToInternalList.get(fileId)
        .getCacheList();
    return getKey(begin, end, fileId, cacheList.iterator(), 0);
  }

  public CacheUnit getKeyByReverse2(long begin, long end, long fileId) {
    DoubleLinkedList<CacheInternalUnit> cacheList = mFileIdToInternalList.get(fileId)
        .getCacheList();
    PreviousIterator iter = cacheList.previousIterator();
    return getKeyByReverse(begin, end, fileId, iter, BUCKET_LENGTH-1);
  }

  private TempCacheUnit handleUnCoincidence(TempCacheUnit unit, CacheInternalUnit
                                            before, CacheInternalUnit after, int bucketIndex) {

		unit.lockedIndex.add(mLockManager.lockUpgrade(unit.getFileId(), bucketIndex));

		if(before != null && before.mBucketIndex != bucketIndex) {
      mLockManager.lockUpgrade(unit.getFileId(), before.mBucketIndex, bucketIndex - 1, unit);
    }
    if(after != null && after.mBucketIndex != bucketIndex) {
      mLockManager.lockUpgrade(unit.getFileId(), bucketIndex + 1, after.mBucketIndex, unit);
    }
		unit.mBefore = before;
    unit.mAfter = after;
    unit.newSize = unit.getSize();

    return unit;
  }

  /**
   *  search CacheInternalUnit before current
   */
  public TempCacheUnit handleRightCoincidence(TempCacheUnit result, CacheInternalUnit
                                               current, boolean addCache, int bucketIndex) {
    result.lockedIndex.add(mLockManager.lockUpgrade(result.getFileId(), bucketIndex));

    result.mAfter = current.after;
  	long already = 0;

    if(result.getEnd() < current.getEnd()) {
      result.resetEnd(current.getEnd());
    }
    int currentIndex = bucketIndex;
    while(current.before != null && result.getBegin() < current.getBegin()) {
      if (addCache) {
        if(current.mBucketIndex != currentIndex) {
          currentIndex --;
          mLockManager.lockUpgrade(current.getFileId(), current.mBucketIndex, currentIndex ,result);
          currentIndex = current.mBucketIndex;
        }
        result.addResourceReverse(current);
        already += current.getSize();
      }
      current = current.before;
    }
		if(current.before == null) {
			result.mBefore = current;
		}

    if (current.before!= null && result.getBegin() <= current.getEnd()) {
      if (addCache) {
        if(current.mBucketIndex != currentIndex) {
          currentIndex --;
          mLockManager.lockUpgrade(current.getFileId(), current.mBucketIndex, currentIndex, result);
        }
        result.addResourceReverse(current);
        already += current.getSize();
      }
      result.resetBegin(current.getBegin());

      result.mBefore = current.before;
    } else if (current.before != null) {
      result.mBefore = current;
    }
    result.newSize = result.getSize() - already;

    return result;
  }

  public TempCacheUnit handleLeftCoincidence(CacheInternalUnit current,
    TempCacheUnit result, boolean addCache, int bucketIndex) {
    result.lockedIndex.add(mLockManager.lockUpgrade(result.getFileId(), bucketIndex));
		result.mBefore = current.before;
		long already = 0;
    if(result.getBegin() > current.getBegin())
    {
      result.resetBegin(current.getBegin());
    }
    int currentIndex = bucketIndex;
    while(current != null && result.getEnd() > current.getEnd()) {
      if (addCache) {
        if(current.mBucketIndex != currentIndex) {
          currentIndex ++;
          mLockManager.lockUpgrade(current.getFileId(), currentIndex, current.mBucketIndex, result);
        }
        result.addResource(current);
        already += current.getSize();
      }
      if(current.after == null) {
        result.mAfter = null;
      }
      current = current.after;
    }
    if (current != null && result.getEnd() >= current.getBegin()) {
      if(addCache) {
        if(current.mBucketIndex != currentIndex) {
          currentIndex ++;
          mLockManager.lockUpgrade(current.getFileId(), currentIndex, current.mBucketIndex, result);
        }
        result.addResource(current);
        already += current.getSize();
      }
      result.resetEnd(current.getEnd());
      result.mAfter = current.after;
    } else if(current != null) {
      result.mAfter = current;
    }
    result.newSize = result.getSize() - already;
    return result;
  }

  /**
   * add new cache unit to cache list, WITH OUT CACHE CLEAN
   *
   * @param unit
   */
  public CacheInternalUnit addCache(TempCacheUnit unit) {
    // long beginTime = System.currentTimeMillis();
    return mFileIdToInternalList.get(unit.mFileId).addCache(unit);
    //insertTime += (System.currentTimeMillis() - beginTime);
  }

  public void convertCache(TempCacheUnit unit, DoubleLinkedList<CacheInternalUnit> cacheList) {
    CacheInternalUnit result = unit.convertType();
    cacheList.insertBetween(result, unit.mBefore,unit.mAfter);
    //printInfo(unit.mFileId);
  }

  public long computeIncrese(TempCacheUnit unit){
    long addSize = 0;
    CacheInternalUnit result = unit.convertType();
    for(CacheInternalUnit tmp = unit.mBefore.after ; tmp != null && tmp.after != unit.mAfter; tmp = tmp.after) {
      addSize -= tmp.getSize();
    }
    //printInfo(unit.mFileId);
    addSize += result.getSize();
    return addSize;
  }

  public void printInfo(long fileid ) {
    DoubleLinkedList<CacheInternalUnit> cacheList = mFileIdToInternalList.get
			(fileid).getCacheList();
    System.out.println(cacheList.toString());
  }

  public LockManager getLockManager() {
  	return LockManager.INSTANCE;
	}

	public enum LockManager {
  	INSTANCE;
		private ReentrantLock tmplock = new ReentrantLock();
		private ConcurrentHashMap<Long, ConcurrentHashMap<Integer,
			ReentrantReadWriteLock>> mCacheLock = new ConcurrentHashMap<>();
		public void lock(){
			tmplock.lock();
		}

		public void unlock() {
			tmplock.unlock();
		}

		public void initFileLock(long fileId) {
		  if(!mCacheLock.containsKey(fileId)) {
		    mCacheLock.put(fileId, new ConcurrentHashMap<>());
      }
    }

    public synchronized void initBucketLock(long fileId, int bucketIndex) {
			if(!mCacheLock.containsKey(fileId)) {
				mCacheLock.put(fileId, new ConcurrentHashMap<>());
			}
	    mCacheLock.get(fileId).put(bucketIndex, new ReentrantReadWriteLock());
    }

    public synchronized ReentrantReadWriteLock readLock(long fileId, int bucketIndex) {
      ReentrantReadWriteLock lock = mCacheLock.get(fileId).get(bucketIndex);
      lock.readLock().tryLock();
      return lock;
    }

    public synchronized void readUnlock(long fileId, int bucketIndex) {
		  mCacheLock.get(fileId).get(bucketIndex).readLock().unlock();
    }

    public synchronized ReentrantReadWriteLock getLock(long fileId, int
			bucketIndex) {
     return mCacheLock.get(fileId).get(bucketIndex);
		}

    public synchronized void writeUnlock(long fileId, int bucketIndex) {
      mCacheLock.get(fileId).get(bucketIndex).writeLock().unlock();
    }

    public boolean checkLock(int currentIndex, CacheInternalUnit unit) {
		  if(unit.mBucketIndex != -1 && unit.mBucketIndex != currentIndex) {
		    return true;
      }
      return false;
    }

    public synchronized ReentrantReadWriteLock.WriteLock lockUpgrade(long fileId, int index) {
      ReentrantReadWriteLock lock = mCacheLock.get(fileId).get(index);
      lock.readLock().unlock();
      lock.writeLock().lock();
      return lock.writeLock();
    }

    public synchronized void lockUpgrade(long fileId, int beginIndex , int EndIndex, TempCacheUnit unit) {
      for (int i = beginIndex; i <= EndIndex; i++) {
				ReentrantReadWriteLock lock = mCacheLock.get(fileId).get(i);
				lock.readLock().unlock();
				lock.writeLock().lock();
        unit.lockedIndex.add(lock.writeLock());
			}
    }

    public synchronized void lockUpgrade(long fileId, int beginIndex , int EndIndex, CacheInternalUnit unit) {
      for(int i = beginIndex;  i <= EndIndex; i ++) {
        ReentrantReadWriteLock lock = mCacheLock.get(fileId).get(i);
        lock.readLock().unlock();
        lock.writeLock().lock();
      }
    }

    public synchronized void writeLock(long fileId, int beginIndex , int
			EndIndex, ArrayList<ReentrantReadWriteLock> l) {
			for(int i = beginIndex;  i <= EndIndex && i > 0; i ++) {
        ReentrantReadWriteLock lock = mCacheLock.get(fileId).get(i);
        lock.writeLock().lock();
        l.add(lock);
      }
    }

		public List<ReentrantReadWriteLock> deleteLock(CacheInternalUnit unit) {

			ArrayList<ReentrantReadWriteLock> l = new ArrayList<>();
			ReentrantReadWriteLock rwLock = mLockManager.getLock(unit
				.getFileId(), unit.mBucketIndex);
			l.add(rwLock);

			rwLock.writeLock().lock();

			if(unit.before != null && unit.before.mBucketIndex != unit.mBucketIndex) {
				mLockManager.writeLock(unit.getFileId(), unit.before.mBucketIndex, unit
					.mBucketIndex -1, l);
			}

			if(unit.after != null && unit.after.mBucketIndex != unit.mBucketIndex) {
				mLockManager.writeLock(unit.getFileId(), unit
					.mBucketIndex + 1, unit.after.mBucketIndex, l);
			}

			return l;
		}
	}

  /*
  public void manageCache(byte[] b, int off, int len, long fileId,CacheInternalUnit unit){
    FileCacheUnit tmpFileCache = mFileIdToInternalList.get(fileId);
    DoubleLinkedList<CacheInternalUnit> cacheList = tmpFileCache.cacheList;
    List<CacheInternalUnit> tmpCache = unit.tmpBuffer;
    if(unit.getBegin() <= tmpCache.get(0).getBegin()
        && unit.getEnd() >= tmpCache.get(tmpCache.size()-1).getEnd()) {
      unit.initCache(b, off, len);
    } else if(unit.getBegin() > tmpCache.get(0).getBegin()) {

    } else {

    }
  }

  public Pair<Integer, Integer> getKey(int begin, int end) {
    if(internalList.size() == 0 ) {
      internalList.add(new Pair<Integer, Integer>(begin, end));
    }
    int l1 = 0, l2 = internalList.size() - 1;
    while (l1 <= l2) {
      int mid = (l1 + l2) / 2;
      Pair<Integer, Integer> currentKey = internalList.get(mid);
      if (begin >= currentKey.getFirst() && end <= currentKey.getSecond()) {
        return currentKey;
      } else if (begin >= currentKey.getSecond()) {
        l1 = mid + 1;
      } else if (end <= currentKey.getFirst()) {
        l2 = mid - 1;
      } else if (end > currentKey.getSecond() && begin >= currentKey.getFirst()) {

        handleRightExceeding(begin, end, mid);
        break;
      } else if (end <= currentKey.getSecond() && begin < currentKey.getFirst()) {

        handleLeftExceeding(begin, end, mid);
        break;
      } else if (begin <=currentKey.getFirst() && end >= currentKey.getSecond()){
        handleBigInternal(begin, end, mid);
        break;
      }
    }
    Pair r =  new Pair<>(begin, end);
    if(l1 > l2) {
      insertInternal(r, l2,l1);
    }
    return r;

  }*/


}
