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

import alluxio.Client;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.InStreamOptions;
import alluxio.exception.PreconditionMessage;
import com.google.common.base.Preconditions;
import org.omg.Messaging.SYNC_WITH_TRANSPORT;
import sun.misc.Unsafe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static alluxio.client.file.cache.ClientCacheContext.*;

public class FileInStreamWithCache extends FileInStream {
  protected final ClientCacheContext mCacheContext;
  public CacheManager mCachePolicy;
  long mPosition;
  final long mLength;
  final long mFileId;
  LockManager mLockManager;

  public FileInStreamWithCache(InStreamOptions opt, ClientCacheContext context, URIStatus status) {
    super(status, opt, FileSystemContext.INSTANCE);
    mCacheContext = context;
		mCachePolicy = mCacheContext.getCacheManager();
		mPosition = 0;
		mLength = status.getLength();
		mFileId = status.getFileId();
		mLockManager = mCacheContext.getLockManager();
	}

  public long getPos() {
  	return mPosition;
	}

	@Override
	public int positionedRead(long pos, byte[] b, int off, int len) throws IOException {
  	return read0(pos, b, off, len);
	}

	public int innerRead(byte[] b, int off, int len) throws  IOException {
  	int res =  super.read(b, off, len);
  	if(res > 0) mPosition += res;

		return res;
	}

	public int innerPositionRead(long pos, byte[] b, int off, int len) throws IOException {
    long begin = System.currentTimeMillis();
    int res =  super.positionedRead(pos, b, off, len);
		missSize += res;
    ClientCacheContext.testTime += System.currentTimeMillis() - begin;
    return res;
	}


	protected int read0(long pos, byte[] b, int off, int len) throws IOException {
    boolean isPosition = false;
  	if(pos != mPosition) {
			isPosition = true;
		}
  	long length = mLength;
		if (pos < 0 || pos >=  length) {
			return -1;
		}
		int res;

		if (len == 0) {
			return 0;
		} else if (pos == mLength) { // at end of file
			return -1;
		}
		if (mLockManager.evictCheck()) {
      try {
				CacheUnit unit = mCacheContext.getCache(mFileId, mLength, pos, Math.min(pos +
					len, mLength));
				if (unit.isFinish()) {
					if (pos < unit.getBegin()) {
						throw new RuntimeException(pos + " " + (pos +
							len) + unit);
					}
					int remaining = mCachePolicy.read((CacheInternalUnit) unit, b, off, pos,
						len);
					if (!isPosition) {
						mPosition += remaining;
					}
          List<Character> l = new ArrayList<>() ;
					l.add((char)('a' +1));
					return remaining;
					//return -1;
				} else {
          TempCacheUnit tmpUnit = (TempCacheUnit) unit;
          if(mCacheContext.isAllowCache()) {
            tmpUnit.setInStream(this);
            res = mCachePolicy.read(tmpUnit, b, off, len, pos, true);
            if (res != len) {
              // the end of file
              tmpUnit.resetEnd((int) mLength);
            }
          } else {
            res = mCachePolicy.read(tmpUnit, b, off, len, pos, false);
            tmpUnit = null;
          }
				}
			} finally {
        mLockManager.evictReadUnlock();
			}
		}
    else{
      if (isPosition) {
        res = innerPositionRead(pos, b, off, len);
      } else {
        res = innerRead(b, off, len);
      }
    }
    return res;
  }

	@Override
  public int read(byte[] b, int off, int len) throws IOException {
    Preconditions.checkArgument(b != null, PreconditionMessage.ERR_READ_BUFFER_NULL);
    Preconditions.checkArgument(off >= 0 && len >= 0 && len + off <= b.length,
        PreconditionMessage.ERR_BUFFER_STATE.toString(), b.length, off, len);
    return read0(mPosition, b, off, len);
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
    } else if (mPosition == mLength) { // at end of file
      return -1;
    }
    long pos = mPosition;
    CacheUnit unit = mCacheContext.getCache(mFileId, mLength, pos, Math.min(pos +
			len, pos + (int)(mLength - mPosition)));
    if(unit.isFinish()) {
      int remaining = ((CacheInternalUnit)unit).positionedRead(b, off, pos, len);
      if (remaining > 0) mPosition += remaining;
      return remaining;
      //return -1;
    }
    else {
      int res = innerRead(b,off,len);
      if(res > 0) {
        TempCacheUnit tempUnit = (TempCacheUnit) unit;
        if(res!=len) {
          tempUnit.resetEnd(mLength);
          mCacheContext.REVERSE = false;
        }
        tempUnit.addCache(b, off, res);
        mCacheContext.addCache(tempUnit);
      } else {
        mCacheContext.REVERSE = false;
      }
      return res;
    }
  }

  @Override
  public long skip(long n) throws IOException {
    long skipLength = super.skip(n);
    if(skipLength >0) {
			mPosition += skipLength;
		}
    return skipLength;
  }

	public int read(byte[] b, int pos, int off, int len) throws IOException {
		int readLen = innerPositionRead(pos,b, off, len);
  	mCachePolicy.promotionFliter(mFileId, pos, pos + readLen);
  	return readLen;
	}
}

