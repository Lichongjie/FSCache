package alluxio.client.file.cache;

import org.omg.Messaging.SYNC_WITH_TRANSPORT;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class CacheManager {
  protected static final ClientCacheContext mCacheContext = ClientCacheContext.INSTANCE;
  private CachePolicy evictor;
	public static long mReadTime = 0;
  public static long mInsertTime = 0;
  private ClientCacheContext.LockManager mLockManager;

	public CacheManager() {
    setPolicy();
		mLockManager = mCacheContext.getLockManager();
  }

  public void setPolicy() {
		evictor = CachePolicy.factory.create(CachePolicy.PolicyName.ISK);
		evictor.init(mCacheContext.mCacheLimit + mCacheContext.CACHE_SIZE
			, mCacheContext);
	}

  public CachePolicy.PolicyName getCurrentPolicy() {
		return evictor.getPolicyName();
	}

  public int read(TempCacheUnit unit, byte[] b, int off, int readlen, long pos)
		throws IOException {
		long begin = System.currentTimeMillis();
		int res = -1;
		res = unit.lazyRead(b, off, readlen, pos);
		long min = System.currentTimeMillis();
		mReadTime += System.currentTimeMillis() - begin;

		BaseCacheUnit unit1 = new BaseCacheUnit(pos,pos + res , unit.getFileId());
		unit1.setCurrentHitVal(unit.getNewCacheSize());
		CacheInternalUnit resultUnit = mCacheContext.addCache(unit);

		for(int index: unit.lockedIndex ) {
			mLockManager.writeUnlock(unit.getFileId(), index);
		}
		evictor.fliter(resultUnit, unit1);
		mInsertTime += System.currentTimeMillis() - min;
		evictor.check(unit);
		return res;
  }

  public int read(CacheInternalUnit unit, byte[] b, int off, long pos, int
		len) {
		long begin = System.currentTimeMillis();
		int remaining = unit.positionedRead(b, off, pos, len);
		mReadTime += System.currentTimeMillis() - begin;
		mLockManager.readUnlock(unit.getFileId(), unit.readLock);
		BaseCacheUnit currentUnit = new BaseCacheUnit(pos, Math.min(unit.getEnd(), pos + len),
				unit.getFileId());
		evictor.fliter(unit, currentUnit);
		return remaining;
	}
}
