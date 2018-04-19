/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.file.cache;


import alluxio.client.file.FileInStream;
import alluxio.client.file.URIStatus;
import alluxio.exception.PreconditionMessage;
import com.google.common.base.Preconditions;

import java.io.IOException;

public class FileInStreamWithCache extends FileInStream {
  protected final ClientCacheContext mCacheContext;
  public static FileInStream mIn;
  public CacheManager mCachePolicy;
  public static URIStatus mStatus;

  public long searchTime() {
    return mCacheContext.getSearchTime();
  }

  public long insertTime(){
    return mCacheContext.getInsertTime();
  }

  public FileInStreamWithCache(FileInStream in, ClientCacheContext context) {
    super();
    mIn = in;
    mCacheContext = context;
		mCachePolicy = mCacheContext.CACHE_POLICY;
		mStatus = mIn.mStatus;
  }

	@Override
	public int positionedRead(long pos, byte[] b, int off, int len) throws IOException {
		return read0(pos, b, off, len);
	}

	private int read0(long pos, byte[] b, int off, int len) throws IOException {
		boolean isPosition = false;
  	if(pos != mIn.mPosition) {
			isPosition = true;
		}
  	long length = mIn.mStatus.getLength();
		if (pos < 0 || pos >=  length) {
			return -1;
		}

		if (len == 0) {
			return 0;
		} else if (pos == mIn.mStatus.getLength()) { // at end of file
			return -1;
		}
		CacheUnit unit = mCacheContext.getCache(mIn.mStatus, pos, Math.min(pos + len, pos + (int)mIn
			.remaining()));
		if(unit.isFinish()) {
			int remaining = mCachePolicy.read((CacheInternalUnit)unit, b, off, pos,
				len);
			if(!isPosition) {
				mIn.mPosition += remaining;
			}
			return remaining;
			//return -1;
		}
		else {
			int res;
			if(unit.getBegin() == 49283072) {
				System.out.println("read new" + unit.getEnd());
			}
			if (mCacheContext.mAllowCache) {
				TempCacheUnit tmpUnit = (TempCacheUnit) unit;
				tmpUnit.setInStream(mIn);
				res = mCachePolicy.read(tmpUnit, b, off, len, pos);
				if (res != len) {
					// the end of file
					tmpUnit.resetEnd((int) mIn.mStatus.getLength());
				}
			} else {
				for(int index: ((TempCacheUnit)unit).lockedIndex ) {
					mCacheContext.getLockManager().writeUnlock(unit.getFileId(), index);
				}
				if (isPosition) {
					res = mIn.positionedRead(pos, b, off, len);
				} else {
					res = mIn.read(b, off, len);
				}
			}

			return res;
		}
	}

	@Override
  public int read(byte[] b, int off, int len) throws IOException {
    Preconditions.checkArgument(b != null, PreconditionMessage.ERR_READ_BUFFER_NULL);
    Preconditions.checkArgument(off >= 0 && len >= 0 && len + off <= b.length,
        PreconditionMessage.ERR_BUFFER_STATE.toString(), b.length, off, len);
    return read0(mIn.mPosition, b, off, len);
  }

  /**
   * sequential reading from file, no data coincidence in  reading process;
   * cached data after reading first time;
   * used by test sequential reading.
   */
  public int sequentialReading(byte[] b, int off, int len) throws IOException {
    Preconditions.checkArgument(b != null, PreconditionMessage.ERR_READ_BUFFER_NULL);
    Preconditions.checkArgument(off >= 0 && len >= 0 && len + off <= b.length,
        PreconditionMessage.ERR_BUFFER_STATE.toString(), b.length, off, len);
    if (len == 0) {
      return 0;
    } else if (mIn.mPosition == mIn.mStatus.getLength()) { // at end of file
      return -1;
    }
    int pos = (int)mIn.mPosition;
    CacheUnit unit = mCacheContext.getCache(mIn.mStatus, pos, Math.min(pos + len, pos + (int)mIn
        .remaining()));
    //TempCacheUnit unit = new TempCacheUnit(1,1,0);
    if(unit.isFinish()) {
      int remaining = ((CacheInternalUnit)unit).positionedRead(b, off, pos, len);
      mIn.mPosition += remaining;

      return remaining;
      //return -1;
    }
    else {
      int res = mIn.read(b,off,len);
      if(res > 0) {
        TempCacheUnit tempUnit = (TempCacheUnit) unit;
        if(res!=len) {
          // the end of file
          tempUnit.resetEnd((int)mIn.mStatus.getLength());
          mCacheContext.REVERSE = false;
        }
        tempUnit.addCache(b, off, res);
        mCacheContext.addCache(tempUnit);
        //System.out.print("add");
      } else {
        mCacheContext.REVERSE = false;
      }
      return res;
    }
  }

  @Override
  public long skip(long n) throws IOException {
    return mIn.skip(n);
  }

  /*
  @Override
  public int readInternal(byte[] b, int off, int len) throws IOException {
    Preconditions.checkArgument(b != null, PreconditionMessage.ERR_READ_BUFFER_NULL);
    Preconditions.checkArgument(off >= 0 && len >= 0 && len + off <= b.length,
        PreconditionMessage.ERR_BUFFER_STATE.toString(), b.length, off, len);
    if (len == 0) {
      return 0;
    } else if (remainingInternal() <= 0) {
      return -1;
    }

    int bytesLeftToRead = len;

    int begin = (int)mPos;
    int end = (int)mPos + len;
    if(end > mFileLength) {
      end = (int)mFileLength;
    }
    int resultPos = off;
    CacheUnit unit = mCacheContext.getCache(mStatus,begin,end);
    // the read range in the unit
    if(unit.isFinish()) {
      int remaining = ((CacheInternalUnit)unit).positionedRead(b, off, begin, end);
      return remaining;
    }
    else {
      TempCacheUnit tempUnit = (TempCacheUnit)unit;
      if(tempUnit.hasResource()) {
        CacheInternalUnit current = tempUnit.getResource();
        boolean beyondCacheList = false;
        while(mPos <= end) {
          // read from temp cache list
          if(mPos >= current.getBegin()) {
            int readLength = current.positionedRead(b,resultPos,(int)mPos, bytesLeftToRead);
            //unit.addCache(current);
            mPos += readLength;
            resultPos += readLength;
            bytesLeftToRead -= readLength;
            tempUnit.consumeResource();
            if(tempUnit.hasResource()) {
              current = tempUnit.getResource();
            } else {
              beyondCacheList = true;
            }
          }
          //read from fileinstream
          else {
            int readLength, needreadLen;
            if(!beyondCacheList) {
              needreadLen = current.getBegin() - (int)mPos;
            } else {
              needreadLen = end - (int)mPos;
            }
            readLength = super.readInternal(b,resultPos, needreadLen);
            if(readLength != -1) {
              tempUnit.addCache(b,resultPos ,readLength);
              resultPos += readLength;
              bytesLeftToRead -= readLength;
            }
          }
        }
      }
      else {
        int readLength = super.readInternal(b,off, len);
        if(readLength > 0) {
          tempUnit.addCache(b, off, readLength);
        }
      }
      mCacheContext.convertCache(tempUnit);
    }

    if (bytesLeftToRead == len && mPos == mFileLength) {
      // Nothing was read, and the underlying stream is done.
      return -1;
    }

    return len - bytesLeftToRead;
  }
  */
}

