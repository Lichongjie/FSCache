package alluxio.client.file.cache;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

public class LRUPolicy implements CachePolicy {
	public LinkedList<CacheInternalUnit> mAccessRecords;
	private Set<CacheUnit> mAccessSet;
	protected long mCacheCapacity;
	protected ClientCacheContext mContext;
	protected long mCacheSize;
	protected long mNeedDelete;
	private PolicyName mPolicyName = PolicyName.LRU;

	public PolicyName getPolicyName() {
		return mPolicyName;
	}

	@Override
	public boolean isSync() {
		return true;
	}

	@Override
	public void init(long cacheSize, ClientCacheContext context) {
		mCacheCapacity = cacheSize;
		mContext = context;
		mCacheSize = 0;
		mAccessRecords = new LinkedList<>();
		mAccessSet = new HashSet<>();
	}

  @Override
	public synchronized void fliter(CacheInternalUnit unit, BaseCacheUnit unit1) {
		if(mAccessSet.contains(unit)) {
			mAccessRecords.remove(unit);
		} else {
			mAccessSet.add(unit);
		}
		mAccessRecords.addLast(unit);
	}

	public synchronized void check(TempCacheUnit unit){
		mCacheSize += unit.getNewCacheSize();
		if (unit.newSize + mCacheSize > mCacheCapacity) {
			mCacheSize -= evict0(mCacheCapacity, unit.newSize + mCacheSize);
		}
	}

	protected long evict0(long cachesize, long limit) {
		final long res;
		mNeedDelete = cachesize - limit;
		res = evict();
		return res;
	}

	@Override
	public long evict() {
		long deleteSize = 0;
		while(mNeedDelete > 0) {
			CacheInternalUnit unit =  mAccessRecords.getFirst();
			mAccessRecords.remove(unit);
			long delete = mContext.delete(unit);
			mNeedDelete -= delete;
			deleteSize += delete;
		}
		return deleteSize;
	}

	@Override
	public void clear() {


	}
}
