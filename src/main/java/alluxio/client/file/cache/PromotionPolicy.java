package alluxio.client.file.cache;

import alluxio.client.file.cache.struct.LongPair;
import alluxio.client.file.cache.submodularLib.ISK;
import alluxio.client.file.cache.submodularLib.IterateOptimizer;
import alluxio.client.file.cache.submodularLib.cacheSet.CacheSet;
import alluxio.client.file.cache.submodularLib.cacheSet.CacheSetUtils;
import alluxio.client.file.cache.submodularLib.cacheSet.GR;
import alluxio.exception.AlluxioException;

import java.io.IOException;
import java.util.*;

public class PromotionPolicy implements Runnable {
  private long mCacheCapacity;
  private IterateOptimizer<CacheUnit> mOptimizer;
  public CacheSet mInputSpace1;
  public CacheSet mInputSpace2;
  public volatile boolean useOne = true;
  public boolean isProtomoting = false;
  private final Object mAccessLock = new Object();
  private final ClientCacheContext mContext = ClientCacheContext.INSTANCE;

  public void setPolicy(CachePolicy.PolicyName name) {
    if (name == CachePolicy.PolicyName.ISK) {
      mOptimizer = null;
      mOptimizer = new ISK((mCacheCapacity), new CacheSetUtils());
    } else if (name == CachePolicy.PolicyName.GR) {
      mOptimizer = null;
      mOptimizer = new GR((mCacheCapacity), new CacheSetUtils());
    }
  }

  public CacheSet move(CacheSet s1, CacheSet s2) {
    for (long fileId : s2.cacheMap.keySet()) {
      Set<CacheUnit> s = s2.get(fileId);
      if (!s1.cacheMap.containsKey(fileId)) {
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

  public void fliter(BaseCacheUnit unit1) {
    synchronized (mAccessLock) {
      if (useOne) {
        mInputSpace1.add(unit1);
        mInputSpace1.addSort(unit1);
      } else {
        mInputSpace2.add(unit1);
        mInputSpace2.addSort(unit1);
      }
    }
  }

  public void promote() {
    isProtomoting = true;
    synchronized (mAccessLock) {
      if (useOne) {
        mOptimizer.addInputSpace(mInputSpace1);
      } else {
        mOptimizer.addInputSpace(mInputSpace2);
      }
      useOne = !useOne;
    }
    mOptimizer.optimize();
    CacheSet result = (CacheSet) mOptimizer.getResult();
    mOptimizer.clear();

    try {
      mContext.merge(result);
    } catch (IOException | AlluxioException e) {
      e.printStackTrace();
    } finally {

    }
  }

  private boolean promoteCheck() {

  }

  public void init(long limit) {
    mCacheCapacity = limit;
  }

  @Override
  public void run() {
    while (true) {
      try {
        if (promoteCheck()) {
          promote();
        }
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

}
