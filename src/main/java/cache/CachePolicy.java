package cache;

public interface CachePolicy {

	public boolean isSync();

	public void init(long cacheSize, ClientCacheContext context);

	public void fliter(CacheInternalUnit unit, BaseCacheUnit unit1);

	public void check(TempCacheUnit unit);

	public long evict();
}
