package cache.submodularLib.cacheSet;

import alluxio.client.file.cache.*;
import alluxio.client.file.cache.struct.DoubleLinkedList;
import javafx.util.Pair;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DivideGR implements CachePolicy{
  public PriorityQueue<BaseCacheUnit> hitRatioQueue = new PriorityQueue<>(new Comparator<BaseCacheUnit>() {
    @Override
    public int compare(BaseCacheUnit o1, BaseCacheUnit o2) {
      return (int)((o1.getHitValue() - o2.getHitValue()) * 10);
    }
  });
  ClientCacheContext mContext;
  private long mCacheCapacity;
  private long mNeedDelete;
  private long mCacheSize;
  private ClientCacheContext.LockManager mLockManager;

  @Override
  public void init(long cacheSize, ClientCacheContext context) {
		mCacheCapacity = cacheSize;
		mCacheSize = 0;
		mContext = context;
		mLockManager = mContext.getLockManager();
	}

	@Override
	public void check(TempCacheUnit unit) {
		mCacheSize += unit.getNewCacheSize();
		if (unit.newSize + mCacheSize > mCacheCapacity) {
			mCacheSize -= evict0(mCacheCapacity, unit.newSize + mCacheSize);
		}
	}

  private void changeHitValue(BaseCacheUnit unit, BaseCacheUnit tmp, Set<BaseCacheUnit> changeSet) {
    long coinEnd = Math.min(tmp.getEnd(), unit.getEnd());
    long coinBegin = Math.max(tmp.getBegin(), unit.getBegin());
    int coincideSize = (int) (coinEnd - coinBegin);
    double coinPerNew = coincideSize / (double) (unit.getEnd() - unit.getBegin());
    double coinPerOld = coincideSize / (double) (tmp.getEnd() - tmp.getBegin());
    tmp.setCurrentHitVal(tmp.getHitValue() + Math.min(coinPerOld, 1) * 1);
    changeSet.add(tmp);
    if (coinPerNew < 1) {
      unit.setCurrentHitVal(unit.getHitValue()+ coinPerNew * 1);
      changeSet.add(unit);
    }
  }

  private void deleteHitValue(BaseCacheUnit unit, BaseCacheUnit tmp, Set<BaseCacheUnit> changeSet) {
    long coinEnd = Math.min(tmp.getEnd(), unit.getEnd());
    long coinBegin = Math.max(tmp.getBegin(), unit.getBegin());
    int coincideSize = (int) (coinEnd - coinBegin);
    double coinPerOld = coincideSize / (double) (tmp.getEnd() - tmp.getBegin());
    tmp.setCurrentHitVal(tmp.getHitValue() + Math.min(coinPerOld, 1) * 1);
    changeSet.add(tmp);
  }

  public long deleteCache(CacheInternalUnit current) {
    FileCacheUnit unit1 = mContext.mFileIdToInternalList.get(current.getFileId());
    Queue<Pair<Long, Long>> q = new LinkedList<>();
    cacheCoinFiliter(current.accessRecord, q);
    current.split(q, unit1.mBuckets);

    q.clear();

    long deleteSize = current.getDeleteSize();
    if(deleteSize > 0) {
      unit1.mBuckets.delete(current);
    }
    DoubleLinkedList<CacheInternalUnit> cacheList = unit1.getCacheList();
    cacheList.delete(current);
    return deleteSize;
  }

  public void cacheCoinFiliter(TreeSet<BaseCacheUnit> set,
                               Queue<Pair<Long, Long>> tmpQueue ){
    long maxEnd = -1;
    long minBegin = -1;
    Iterator<BaseCacheUnit> iter = set.iterator();
    while(iter.hasNext()) {
      BaseCacheUnit tmpUnit = iter.next();
      if(minBegin == -1) {
        minBegin = tmpUnit.getBegin();
        maxEnd = tmpUnit.getEnd();
      } else {
        if(tmpUnit.getBegin() <= maxEnd) {
          maxEnd = Math.max(tmpUnit.getEnd() , maxEnd);
        }
        else {
          tmpQueue.add(new Pair<>(minBegin, maxEnd));
          minBegin = tmpUnit.getBegin();
          maxEnd = tmpUnit.getEnd();
        }
      }
    }
    tmpQueue.add(new Pair<>(minBegin, maxEnd));
  }

  private void addReCompute(CacheInternalUnit unit, BaseCacheUnit current) {

  	TreeSet<BaseCacheUnit> unitQueue = unit.accessRecord;
    Set<BaseCacheUnit> changeSet = new HashSet<>();
    boolean isIn = false;

    Iterator<BaseCacheUnit> iter = unitQueue.iterator();
    while(iter.hasNext()) {
      BaseCacheUnit tmp = iter.next();
      if(current.getEnd() <= tmp.getBegin()) {
        break;
      }
      if(current.isCoincience(tmp)) {
        changeHitValue(current, tmp, changeSet);
      } else if(current.getBegin() == tmp.getBegin() &&
          current.getEnd() == tmp.getEnd()) {
        tmp.setCurrentHitVal(tmp.getHitValue() + 1);
        current.setCurrentHitVal(1);
        changeSet.add(tmp);
        isIn = true;
      }
    }

    if(!isIn) {
      unitQueue.add(current);
      changeSet.add(current);
    }

    for(BaseCacheUnit resUnit : changeSet) {
      if(hitRatioQueue.contains(resUnit)) {
        hitRatioQueue.remove(resUnit);
      }
      hitRatioQueue.offer(resUnit);
    }
  }

  private void deleteReCompute(CacheInternalUnit unit, BaseCacheUnit current) {
    TreeSet<BaseCacheUnit> unitQueue = unit.accessRecord;
    Set<BaseCacheUnit> changeSet = new HashSet<>();

    Iterator<BaseCacheUnit> iter = unitQueue.iterator();
    while(iter.hasNext()) {
      BaseCacheUnit tmp = iter.next();
      if(current.getEnd() <= tmp.getBegin()) {
        break;
      }
      if(current.isCoincience(tmp)) {
        deleteHitValue(current, tmp, changeSet);
      } else if(current.getBegin() == tmp.getBegin() &&
          current.getEnd() == tmp.getEnd()) {
        tmp.setCurrentHitVal(tmp.getHitValue() - 1);
        changeSet.add(tmp);
      }
    }

    for(BaseCacheUnit resUnit : changeSet) {
      if(hitRatioQueue.contains(resUnit)) {
        hitRatioQueue.remove(resUnit);
      }
      hitRatioQueue.add(resUnit);
    }
  }

  public long evict0(long cachesize, long limit) {
		final long res;
		synchronized (this) {
			mNeedDelete = cachesize - limit;
			res = evict();
		}
		return res;
	}

	@Override
  public long evict() {
  	long delete = 0;
    while(mNeedDelete > 0) {
			BaseCacheUnit baseUnit = hitRatioQueue.poll();
			CacheInternalUnit unit = (CacheInternalUnit)ClientCacheContext.INSTANCE.
				mFileIdToInternalList.get(baseUnit.getFileId()).getKeyFromBucket
				(baseUnit.getBegin(), baseUnit.getEnd());
			// change read lock to write lock
			List<ReentrantReadWriteLock> l = null;
      try {
				l = mLockManager.deleteLock(unit);
        unit.accessRecord.remove(baseUnit);
        deleteReCompute(unit, baseUnit);
        long deletetmp = deleteCache(unit);
        delete += deletetmp;
        mNeedDelete -= deletetmp;
      } finally {
        if( l != null) {
        	for(ReentrantReadWriteLock lock : l) {
        		lock.writeLock().unlock();
					}
				}
      }
		}
		return  delete;
  }

  @Override
  public void fliter(CacheInternalUnit unit, BaseCacheUnit current) {
  	addReCompute(unit, current);
    unit.accessRecord.add(current);
  }

	@Override
	public boolean isSync() {
		return false;
	}



}
