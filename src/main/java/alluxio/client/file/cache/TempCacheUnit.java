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
import alluxio.client.file.cache.struct.LinkNode;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class TempCacheUnit extends LinkNode<TempCacheUnit> implements CacheUnit {
  long mFileId;
  private long mBegin;
  private long mEnd;
  public Deque<CacheInternalUnit> mCacheConsumer = new LinkedList<>();
  //private CompositeByteBuf mData;
  private List<ByteBuf> mData = new LinkedList<>();
  CacheInternalUnit mBefore;
  CacheInternalUnit mAfter;
  public FileInStreamWithCache in;
  private long mSize;
  private long mNewCacheSize;
  private long mRealReadSize;
  public List<Integer> lockedIndex = new LinkedList<>();
  public TreeSet<BaseCacheUnit> mTmpAccessRecord = new TreeSet<>(new Comparator<CacheUnit>() {
    @Override
    public int compare(CacheUnit o1, CacheUnit o2) {
      return (int)(o1.getBegin() - o2.getBegin());
    }
  });

  public Queue<CacheInternalUnit> deleteQueue = new LinkedList<>();

  public long newSize;

  public long getSize() {
    return mSize;
  }

  public long getNewCacheSize() {
  	return mNewCacheSize;
	}

  public long getRealReadSize() {
  	return mRealReadSize;
	}

  @Override
  public long getFileId() {
    return mFileId;
  }

  @Override
  public long getBegin() {
    return mBegin;
  }

  public TempCacheUnit(){}

	public void init(long begin, long end, long fileId) {
    mBegin = begin;
    mEnd = end;
    mFileId = fileId;
    mSize = mEnd - mBegin;
    mNewCacheSize = 0;
	}

  public TempCacheUnit(long begin, long end, long fileId) {
    mBegin = begin;
    mEnd = end;
    mFileId = fileId;
    mSize = mEnd - mBegin;
    mNewCacheSize = 0;
   // mData = ByteBufAllocator.DEFAULT.compositeBuffer();
  }

  public void setInStream(FileInStreamWithCache i) {
    in = i;
  }

  public void resetEnd(long end) {
    mEnd = end;
  }

  public void resetBegin(long begin) {
    mBegin = begin;
  }

  public long getEnd() {
    return mEnd;
  }

  public void consumeResource(boolean isCache) {
    CacheInternalUnit unit = mCacheConsumer.poll();
     // (TODO) maybe cause array copy
    if(isCache) {
      List<ByteBuf> tmp = unit.getAllData();
      mData.addAll(tmp);
      mTmpAccessRecord.addAll(unit.accessRecord);
      deleteQueue.add(unit);
    }
  }

  /**
   * Read from file or cache, don't cache read data to cache List
   */
  public int read(byte[] b ,int off, int len) throws IOException {
    long pos = mBegin;
    long end = Math.min(mEnd, mBegin + len);
    int leftToRead = (int)(end - mBegin);
    mRealReadSize = leftToRead;
    int distPos = off;
    if(hasResource()) {
      CacheInternalUnit current = getResource();
      boolean beyondCacheList = false;
      int readLength;
      while(pos <= end) {
        //read from cache
        if(pos >= current.getBegin()) {
          readLength = current.positionedRead(b, distPos, pos, leftToRead);
          if(hasResource()) {
            current = getResource();
          } else {
            beyondCacheList = true;
          }
        }
        //read from File, the need byte[] is before the current CacheUnit
        else {
          int needreadLen;
          if(!beyondCacheList) {
            needreadLen =(int) (current.getBegin() - pos);
          } else {
            needreadLen = (int)(end - pos);
          }
          readLength = in.innerRead(b, distPos, needreadLen);
        }
        // change read variable
        if(readLength != -1) {
          pos += readLength;
          distPos += readLength;
          leftToRead -= readLength;
        }
      }
      return distPos - off;
    }
    else {
      return in.innerRead(b,off, leftToRead);
    }
  }

  /**
   * Read from file or cache, cache data to cache List
   */
  public int lazyRead(byte[] b, int off, int len, long readPos, boolean isCache)
    throws IOException {
    boolean positionedRead = false;
    if (readPos != in.getPos()) {
      positionedRead = true;
    }
    long pos = readPos;
    long end = Math.min(mEnd, readPos + len);
    int leftToRead = (int) (end - readPos);
    mRealReadSize = leftToRead;
    int distPos = off;
    if (hasResource()) {
      CacheInternalUnit current = getResource();
      boolean beyondCacheList = false;
      int readLength = -1;
      while (pos < end) {
      //read from cache
        if (current != null && pos >= current.getBegin()) {
          readLength = current.positionedRead(b, distPos, pos, leftToRead);
          if (readLength != -1 && !positionedRead) {
          	in.skip(readLength);
          }
          consumeResource(isCache);

          if (hasResource()) {
            current = getResource();
          } else {
            beyondCacheList = true;
            current = null;
          }
        }
        //read from File, the need byte[] is before the current CacheUnit
        else {
          int needreadLen;
          needreadLen = (int) (end - pos);
          if (!beyondCacheList) {
            needreadLen = Math.min((int) (current.getBegin() - pos),
							needreadLen);
          }
          if (!positionedRead) {
            readLength = in.innerRead(b, distPos, needreadLen);
          } else {
            readLength = in.innerPositionRead(pos, b, distPos, needreadLen);
          }

          if (readLength != -1 && isCache) {
            addCache(b, distPos, readLength);
            mNewCacheSize += readLength;
          }
        }
        // change read variable
        if (readLength != -1) {
          pos += readLength;
          distPos += readLength;
          leftToRead -= readLength;
        }
      }
      return distPos - off;
    } else {
      int readLength;
      if (!positionedRead) {
        readLength = in.innerRead(b, off, leftToRead);
      } else {
        readLength = in.innerPositionRead(pos, b, off, leftToRead);
      }
      if (readLength > 0 && isCache) {
        addCache(b, off, readLength);
        mNewCacheSize += readLength;
      }
      return readLength;
    }
  }

  public void addResource(CacheInternalUnit unit) {
    if(mCacheConsumer.isEmpty()) {
      mBefore = unit.before;
    }
    mAfter = unit.after;
    mCacheConsumer.add(unit);
  }

  public void addResourceReverse(CacheInternalUnit unit) {
    if(mCacheConsumer.isEmpty()) {
      mAfter = unit.after;
    }
    mBefore = unit.before;
    mCacheConsumer.addFirst(unit);
  }

  public int addCache(FileInStreamWithCache in, int off, int len) throws IOException{
    int cacheSize = ClientCacheContext.CACHE_SIZE;
    int readLen = 0;
    for(int i = off; i < len + off ; i += cacheSize ) {
      if (len + off - i > cacheSize) {
        ByteBuf tmp = PooledByteBufAllocator.DEFAULT.heapBuffer(cacheSize);
        byte[] b = tmp.array();
        readLen += in.innerPositionRead(off, b, 0, b.length);
        mData.add(tmp);
      } else {
        ByteBuf tmp = PooledByteBufAllocator.DEFAULT.heapBuffer(len + off - i);
        byte[] b = tmp.array();
        readLen += in.innerPositionRead(off, b, 0, b.length);
        mData.add(tmp);
      }
    }
    return readLen;
  }

  public int cache(long pos, int len) throws IOException {
    long end = Math.min(mEnd, pos + len);
    int leftToRead = (int) (end - pos);
    mRealReadSize = leftToRead;
    if (hasResource()) {
      CacheInternalUnit current = getResource();
      boolean beyondCacheList = false;
      int readLength = -1;
      while (pos < end) {
        //read from cache
        if (current != null && pos >= current.getBegin()) {
          readLength = Math.min((int)current.getSize(), leftToRead);
          consumeResource(true);
          if (hasResource()) {
            current = getResource();
          } else {
            beyondCacheList = true;
            current = null;
          }
        }
        else {
          int needreadLen = (int) (end - pos);
          if (!beyondCacheList) {
            needreadLen = Math.min((int) (current.getBegin() - pos),
                    needreadLen);
          }
          readLength = addCache(in, (int)pos, needreadLen);
          if (readLength != -1 ) {
            mNewCacheSize += readLength;
          }
        }
        // change read variable
        if (readLength != -1) {
          pos += readLength;
          leftToRead -= readLength;
        }
      }
      return (int)mNewCacheSize;
    } else {
      int readLength = addCache(in, (int)pos, leftToRead);
      if (readLength != -1 ) {
        mNewCacheSize += readLength;
      }
      return (int)mNewCacheSize;
    }
  }

  public void addCache(byte[] b, int off, int len) {
    long begin = System.currentTimeMillis();
    int cacheSize = ClientCacheContext.CACHE_SIZE;
    for(int i = off; i < len + off ; i += cacheSize ) {
      if (len + off - i > cacheSize) {
			  ByteBuf tmp = PooledByteBufAllocator.DEFAULT.heapBuffer(cacheSize);
        tmp.writeBytes(b,i,cacheSize);
        // Unpooled.copiedBuffer()
				//mData.add(Unpooled.copiedBuffer(b, i, cacheSize));
				mData.add(tmp);
      } else {
			  ByteBuf tmp = PooledByteBufAllocator.DEFAULT.heapBuffer(len + off - i);
        tmp.writeBytes(b,i,len + off - i);
        //mData.add(Unpooled.copiedBuffer(b, i, len + off - i));
        mData.add(tmp);
      }
    }
    ClientCacheContext.testTime2 += System.currentTimeMillis() - begin;
  }

  /**
   * This function must called after lazyRead() function called.
   *
   * @return new Cache Unit to put in cache space.
   */
  public CacheInternalUnit convert() {
    while(!mCacheConsumer.isEmpty()) {
      consumeResource(true);
    }

		// the tmp unit become cache unit to put into cache space, so, the data ref need
		// to add 1;
		for(ByteBuf buf : mData) {
			buf.retain();
		}

    CacheInternalUnit result = new CacheInternalUnit(mBegin, mEnd,mFileId, mData);
    result.before = this.mBefore;
    result.after = this.mAfter;
    result.accessRecord.addAll(mTmpAccessRecord);
    return result;
  }

  public CacheInternalUnit convertType() {
    CacheInternalUnit result = new CacheInternalUnit(mBegin, mEnd, mFileId, null);
    return result;
  }

  public CacheInternalUnit getResource() {
		return mCacheConsumer.peek();
  }

  public boolean hasResource() {
		 return !mCacheConsumer.isEmpty();
  }

  @Override
  public boolean isFinish() {
    return false;
  }

  @Override
  public String toString() {
    return "unfinish begin: " + mBegin + "end: " + mEnd + "\n";
  }

  @Override
  public int hashCode() {
    return (int)((this.mEnd * 31 + this.mBegin) * 31 + this.mFileId ) * 31;
  }

  @Override
  public boolean equals(Object obj) {
    if(obj instanceof TempCacheUnit) {
      TempCacheUnit tobj = (TempCacheUnit)obj;
      return this.mBegin == tobj.getBegin() && this.mEnd == tobj.getEnd();
    }
    return false;
  }

  public int compareTo(TempCacheUnit node) {
		if(node.getBegin() >= this.mEnd) {
			return -1;
		}
		else if(node.getEnd() <= this.mBegin) {
			return 0;
		}
		return 0;
  }

}
