package alluxio.client.file.cache;

import alluxio.client.file.cache.struct.LinkNode;

public class BaseCacheUnit extends LinkNode<BaseCacheUnit> implements CacheUnit {
  private long mBegin, mEnd, mFileId;
  private double currentHitVal;
  private long mPureIncrease;

  public BaseCacheUnit(long begin, long end, long fileId) {
    mBegin = begin;
    mEnd = end;
    mFileId = fileId;
    currentHitVal = 1;
    mPureIncrease = 0;
  }
  public long getPureIncrease() {
    return mPureIncrease;
  }

  public void setPureIncrease(long increase) {
    mPureIncrease = increase;
  }

  public long getIncrease() {
    return mPureIncrease;
  }

  public BaseCacheUnit setCurrentHitVal(double val) {
    currentHitVal = val;
    return this;
  }

  public double getHitValue() {
    return currentHitVal;
  }

  public boolean isFinish() {
    return false;
  }

  public long getBegin() {
    return mBegin;
  }

  public long getEnd() {
    return mEnd;
  }

  public long getFileId() {
    return mFileId;
  }

  public long getSize() {
    return mEnd - mBegin;
  }


  public boolean isCoincience(CacheUnit u2) {
    if (getFileId() != u2.getFileId()) {
      return false;
    } if (mBegin == u2.getBegin() && mEnd == u2.getEnd()) {
      return false;
    } if (mBegin <= u2.getBegin() && mEnd > u2.getBegin()) {
      return true;
    } if (mBegin < u2.getEnd()) {
      return true;
    }
    return false;
	}

  @Override
  public String toString() {
    return "empty unit begin: " + mBegin + "end: " + mEnd ;
  }
  public int compareTo(BaseCacheUnit node) {
    return 0;
  }
}
