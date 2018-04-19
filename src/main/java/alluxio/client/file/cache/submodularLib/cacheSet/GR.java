package alluxio.client.file.cache.submodularLib.cacheSet;

import alluxio.client.file.cache.CacheUnit;
import alluxio.client.file.cache.submodularLib.Subgradient;

import java.util.Iterator;
import java.util.Set;

public class GR extends Subgradient<CacheUnit> {
	CacheHitCalculator mHitCalculator;
	CacheSpaceCalculator mSpaceCalculator;
	double mLimit;

	public GR(long limit, CacheSetUtils utils) {
		super(null, utils);
		mHitCalculator = new CacheHitCalculator(utils);
		mSpaceCalculator = new CacheSpaceCalculator();
    mLimit = limit;
	}

	@Override
	public void addInputSpace(Set<CacheUnit> input) {
		mCandidateSet = input;
	}

	@Override
	public boolean iterateLimit(CacheUnit j) {
		if (mHitCalculator.mSpaceSize <= mLimit) {
			return true;
		}
		return false;
	}

	@Override
	public void init() {
		super.init();
    mHitCalculator.mComputeSpace = true;
    mHitCalculator.init();
		mChainSet = new CacheSet();
	}

	@Override
	public double optimizeObject(CacheUnit j) {
		return mHitCalculator.function(j) ;
	}

	@Override
	public void iterateOptimize() {
		mMaxSubgradient = 0;
		CacheUnit result = null;
		mStopIterate = true;
		mHitCalculator.iterateInit();
		Iterator<CacheUnit> iter = ((CacheSet)mCandidateSet).iterator();
		while (iter.hasNext()) {
			CacheUnit j = iter.next();
      double tmpSubgradient = optimizeObject(j);
      if (iterateLimit(j)) {
        // System.out.println("res " + new CacheSpaceCalculator().function(new CacheSetUtils().union(mChainSet, j)));
        // System.out.println("res1 " + mHitCalculator.mSpaceSize);
        if (tmpSubgradient > mMaxSubgradient) {
					mStopIterate = false;
					mMaxSubgradient = tmpSubgradient;
					result = j;
					mHitCalculator.setMaxMark();
        }
			}
      mHitCalculator.backspace();
    }
		if (result != null) {
			mChainSet.add(result);
      mCandidateSet.remove(result);
			if(!mStopIterate) {
        mHitCalculator.addMaxBase(result);
			}
		}
	}
}

