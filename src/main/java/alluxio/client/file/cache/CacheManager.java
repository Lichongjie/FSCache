package alluxio.client.file.cache;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class CacheManager {
  protected static final ClientCacheContext mCacheContext = ClientCacheContext.INSTANCE;
  private CachePolicy evictor;
	public static long mReadTime = 0;
  public static long mInsertTime = 0;
  private volatile boolean isEvictStart;
  private ClientCacheContext.LockManager mLockManager;
  private ExecutorService COMPUTE_POOL = Executors.newFixedThreadPool(2);

	public CacheManager() {
    isEvictStart = false;
    evictor = new ISKPolicy();
    evictor.init(mCacheContext.mCacheLimit, mCacheContext);
		mLockManager = mCacheContext.getLockManager();
		startSyncEviction();
  }

  public void startSyncEviction() {
		if(!evictor.isSync()) {
			return;
		}
		if(!isEvictStart) {
			synchronized (this) {
				if(!isEvictStart) {
					COMPUTE_POOL.submit((Runnable)evictor);
					isEvictStart = true;
				}
			}
		}
	}

  public int read(TempCacheUnit unit, byte[] b, int off, int readlen, long pos)
		throws IOException {
		long begin = System.currentTimeMillis();
		int res = -1;
		res = unit.lazyRead(b, off, readlen, pos);

		long min = System.currentTimeMillis();
		mReadTime += System.currentTimeMillis() - begin;
		if (res != readlen) {
			// the end of file
			unit.resetEnd((int) unit.in.mStatus.getLength());
			//mCacheContext.REVERSE = false;
		}
		BaseCacheUnit unit1 = new BaseCacheUnit(pos,pos + res , unit.getFileId());
		unit1.setCurrentHitVal(unit.getNewCacheSize());
		CacheInternalUnit resultUnit = mCacheContext.addCache(unit);
		for(int index: unit.lockedIndex ) {
			mCacheContext.getLockManager().writeUnlock(unit.getFileId(), index);
		}
		evictor.fliter(resultUnit, unit1);
		mInsertTime += System.currentTimeMillis() - min;
		evictor.check(unit);
		return res;
  }

  public int read(CacheInternalUnit unit, byte[] b, int off, long pos, int
		len) {
		int remaining = unit.positionedRead(b, off, pos, len);
		unit.readLock.readLock().unlock();

		BaseCacheUnit currentUnit = new BaseCacheUnit(pos, Math.min(unit.getEnd(), pos + len),
				unit.getFileId());
		evictor.fliter(unit, currentUnit);
		return remaining;
	}
}
