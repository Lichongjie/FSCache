package alluxio.client.file.cache;

import alluxio.client.file.cache.submodularLib.ISK;
import alluxio.client.file.cache.submodularLib.cacheSet.CacheSet;
import alluxio.client.file.cache.submodularLib.cacheSet.CacheSetUtils;

import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ISKPolicy implements CachePolicy, Runnable {
	private CacheSet mInputSpace1;
	private CacheSet mInputSpace2;
	private volatile boolean useOne = true;
	private volatile boolean isEvicting = false;
	private ClientCacheContext mContext;
	private ClientCacheContext.LockManager mLockManager;
	private long mCacheCapacity;
	private long mCacheSize;
	private double mLowWaterMark = 0.5;
	private double mHighWaterMark= 0.75;
	private ISK mOptimizer;
	private static long EVICT_THREAD_INTERNAL = 10;
	private final Object mAccessLock = new Object();
	private boolean useGhost;
	private GhostCache mGhost = mContext.getGhostCache();
	private CacheSet newAccessSpace = new CacheSet();
	private ExecutorService mEvictPool = Executors.newSingleThreadExecutor();
  private int evictContious = 0;

	public void useGhost() {
		mContext.mUseGhostCache = true;
		useGhost = true;
	}

	public CacheSet move(CacheSet s1, CacheSet s2) {
	  for(long fileId :s2.cacheMap.keySet()){
	    Set<CacheUnit> s = s2.get(fileId);
	    if(!s1.cacheMap.containsKey(fileId)) {
	      s1.cacheMap.put(fileId, new TreeSet<>(new Comparator<CacheUnit>() {
					@Override
					public int compare(CacheUnit o1, CacheUnit o2) {
						return 0;
					}
				}));
      }
	    s1.get(fileId).addAll(s);
    }
    s2.clear();
    return s1;
  }

	public long evict() {
		System.out.println("evict begin " + mCacheSize + " " + mCacheCapacity + "" +
			" " );
		boolean evictInputSet = useOne;
    isEvicting = true;
		synchronized (mAccessLock) {
      if (useOne) {
				mOptimizer.addInputSpace(mInputSpace1);
      } else {
				mOptimizer.addInputSpace(mInputSpace2);
      }
      useOne = !useOne;
    }
		mOptimizer.optimize();
		CacheSet result = mOptimizer.getResult();

		long delete = 0;
    synchronized (mAccessLock) {
      if (evictInputSet) {
        mInputSpace1.clear();
        move(mInputSpace2, result);
        result = mInputSpace2;
      } else {
        mInputSpace2.clear();
        move(mInputSpace1, result);
        result = mInputSpace1;
      }
      if(useGhost) {
      	mGhost.clear();
			}
			for (long fileId : result.keySet()) {
        Set<CacheUnit> inputSet = result.get(fileId);
        delete += mContext.mFileIdToInternalList.get(fileId).elimiate
            (inputSet);
			}
      mCacheSize -= delete;
      System.out.println("evict end " + mCacheSize);

		}
    isEvicting = false;

		return delete;
	}

	private synchronized boolean evictCheck() {
		if (mCacheSize > mHighWaterMark * mCacheCapacity) {
      return true;
		}
		return false;
	}

	private boolean LowHitRatioCheck() {
		if(useGhost) {
			synchronized (mAccessLock) {
				CacheSet input;
				if(useOne) {
					input = mInputSpace1;
				} else {
					input = mInputSpace2;
				}
				double hitrate = mGhost.statisticsHitRatio(input);
				if ( (hitrate / (double)input.size()) >  mGhost.mGhostHit) {
          return true;
				} else {
					return false;
				}
			}
		} else {
			return false;
		}
	}

	@Override
	public void run() {
		useGhost();
		while (true) {
			try {
				if(evictCheck()) {
					if(!isEvicting) {
						mEvictPool.submit(new Runnable() {
							@Override
							public void run() {
								try {
									evict();
								} finally {
									if(mContext.mAllowCache == false) {
										mContext.mAllowCache = true;
									}
									if(evictCheck()) {
										evictContious ++;
									} else {
										evictContious = 0;
									}
								}
							}
						});
					}
				}
				if(mCacheSize > mCacheCapacity || evictContious >= 2 ) {
					if(mCacheSize > mCacheCapacity) {
						mContext.mAllowCache = false;
					}
					if(mHighWaterMark >= 0.9 || mLowWaterMark <= 0.1) {
						continue;
					}
					if(LowHitRatioCheck()) {
						mLowWaterMark += 0.1;
						if(mHighWaterMark - mLowWaterMark <= 0.2) {
							mHighWaterMark += 0.1;
						}
					} else {
            mHighWaterMark -= 0.1;
						if(mHighWaterMark - mLowWaterMark <= 0.2) {
							mLowWaterMark -= 0.1;
						}
					}
				}
			} finally {
				try {
					Thread.sleep(EVICT_THREAD_INTERNAL);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
		}
	}

	@Override
	public void fliter(CacheInternalUnit unit, BaseCacheUnit unit1) {
	  synchronized (mAccessLock) {
      if (useOne) {
        mInputSpace1.add(unit1);
        mInputSpace1.addSort(unit1);
      } else {
        mInputSpace2.add(unit1);
				mInputSpace2.addSort(unit1);
			}
    }
    if(useGhost) {
			newAccessSpace.add(unit1);
		}
	}

	public boolean isSync() {
		return true;
	}

	public void init(long cacheSize, ClientCacheContext context) {

		mCacheSize = 0 ;
		mCacheCapacity = cacheSize;
		mContext = context;
		mOptimizer = new ISK((long)(mCacheCapacity * mLowWaterMark), new CacheSetUtils());
		mLockManager = mContext.getLockManager();
		mInputSpace1 = new CacheSet();
		mInputSpace2 = new CacheSet();
	}

	public void check(TempCacheUnit unit) {
		mCacheSize += unit.getNewCacheSize();
	}


}
