package alluxio.client.file.cache;

import alluxio.thrift.AlluxioService;
import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;
import org.omg.Messaging.SYNC_WITH_TRANSPORT;

import java.io.IOException;

public final class CacheManager {
  protected static final ClientCacheContext mCacheContext = ClientCacheContext.INSTANCE;
  private CachePolicy evictor;
  public static long mInsertTime = 0;
  private ClientCacheContext.LockManager mLockManager;
  private PromotionPolicy mPolicy;
  private boolean isPromotion;

  public CacheManager() {
    setPolicy();
    mLockManager = mCacheContext.getLockManager();
  }

  public void promotionFliter(long fileId, long begin, long end) {
    mPolicy.fliter(new BaseCacheUnit(fileId, begin, end));
	}

  public void setPolicy() {
    evictor = CachePolicy.factory.create(CachePolicy.PolicyName.ISK);
    evictor.init(mCacheContext.mCacheLimit + mCacheContext.CACHE_SIZE, mCacheContext);
  }

  public CachePolicy.PolicyName getCurrentPolicy() {
    return evictor.getPolicyName();
  }

  public int read(TempCacheUnit unit, byte[] b, int off, int readlen, long
    pos, boolean isAllowCache)
    throws IOException {
    int res = -1;
    long begin = System.currentTimeMillis();
    res = unit.lazyRead(b, off, readlen, pos, isAllowCache);
    if(isAllowCache) {
      BaseCacheUnit unit1 = new BaseCacheUnit(pos, pos + res, unit.getFileId());
      unit1.setCurrentHitVal(unit.getNewCacheSize());
      CacheInternalUnit resultUnit = mCacheContext.addCache(unit);
      evictor.fliter(resultUnit, unit1);
      evictor.check(unit);
    }
    mLockManager.writeUnlockList(unit.getFileId(), unit.lockedIndex);
    return res;
  }

  public int read(CacheInternalUnit unit, byte[] b, int off, long pos, int
    len) {
    int remaining = unit.positionedRead(b, off, pos, len);
    mLockManager.readUnlock(unit.getFileId(), unit.readLock);
    BaseCacheUnit currentUnit = new BaseCacheUnit(pos, Math.min(unit.getEnd(), pos + len),
      unit.getFileId());
    evictor.fliter(unit, currentUnit);
    return remaining;
  }

  public int cache(TempCacheUnit unit, long pos, int len) throws IOException {
    return unit.cache(pos, len);
  }
}
