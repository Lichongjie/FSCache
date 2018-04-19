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

import alluxio.client.file.cache.struct.LinkNode;
import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import javafx.util.Pair;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class CacheInternalUnit extends LinkNode<CacheInternalUnit> implements CacheUnit {
  private long mBegin;
  private long mEnd;
  //private ByteBuf mData;
  public List<ByteBuf> mData = new LinkedList<>();
  private long mFileId;
  private int mCacheSize = ClientCacheContext.CACHE_SIZE;
  private final long mSize;
	private long mDeleteCacheSize = 0;
	public  Set<BaseCacheUnit> accessRecord =  Collections.synchronizedSet(new TreeSet<>(new Comparator<CacheUnit>() {
    @Override
    public int compare(CacheUnit o1, CacheUnit o2) {
      return (int) (o1.getBegin() - o2.getBegin());
    }
	}));
	public volatile ReentrantReadWriteLock readLock;


	public int mBucketIndex = -1;

	public long getSize() {
		return mSize;
  }

  public void initBucketIndex(int index) {
    mBucketIndex = index;
  }

  @Override
  public long getFileId() {
    return mFileId;
  }

  public CacheInternalUnit(long begin, long end, long fileId) {
    mBegin = begin;
    mEnd = end;
    mFileId = fileId;
    mSize = mEnd - mBegin;
  }
  public CacheInternalUnit(long begin, long end, long fileId, List<ByteBuf> data) {
    mBegin = begin;
    mEnd = end;
    mData = data;
    mFileId = fileId;
    mSize = mEnd - mBegin;
  }

  public long getDeleteSize() {
		  return mDeleteCacheSize;
	}


  public long getBegin() {
    return mBegin;
  }

  public long getEnd() {
    return mEnd;
  }

  public boolean isCached() {
    return mData != null;
  }

  /**
   * init the data to b, only being called when tmplist is null.
   *
   * @param b
   * @param off
   * @param len
   */
  public void initCache(byte[] b, int off, int len) {

    //for(int i = off; i < len ;i +=)
  }


  public boolean hasCache() {
    return mData != null;
  }

  public int positionedRead(byte[] b, int off) {
    return positionedRead(b, off, mBegin, (int)(mEnd - mBegin));
  }

 // public ByteBuf getCache() {
   // return mData;
  //}

  /**
   * @param b the result byte array
   * @param off the begin position of byte b
   * @param begin the begin position of file
   * @param len the end position of file
   * @return
   */
  public int positionedRead(byte[] b,int off, long begin, int len) {
    Preconditions.checkArgument(begin >= mBegin);
    if(mData.size() == 0) {
    	System.out.println(begin);
		}
    Preconditions.checkArgument(mData.size() > 0);
    if(begin >= mEnd) {
      return -1;
    }
    long newBegin = mBegin;

    Iterator<ByteBuf> iter = mData.iterator();
    ByteBuf current = iter.next();

    //skip ByteBuf needing not to read.
    while(begin > (newBegin + current.capacity())) {
      newBegin += current.capacity() ;
      current = iter.next();
    }

    int leftToRead =  (int)Math.min(mEnd - begin, len);
    int readedLen = 0;
    // skip the first bytebuf reduntant byte len;
    if(begin > newBegin) {
      int currentLeftCanReadLen = current.capacity() - (int)(begin - newBegin);
      int readLen = Math.min(currentLeftCanReadLen, leftToRead);
      current.getBytes((int)(begin - newBegin), b, off, readLen);
      leftToRead -= readLen;
      readedLen += readLen;
      if(iter.hasNext()) {
        current = iter.next();
      }
    }

    while(leftToRead > 0 ) {
      int readLen = Math.min(current.capacity(), leftToRead);
      current.getBytes(0, b, off + readedLen, readLen);
      leftToRead -= readLen;
      readedLen += readLen;
      if(iter.hasNext()) {
        current = iter.next();
      } else {
        break;
      }
    }

    return readedLen;
  }

  @Override
  public boolean isFinish() {
    return true;
  }

  @Override
  public String toString() {
    return "finish begin: " + mBegin + "end: " + mEnd ;
  }

  private void reset(int newBegin, int newEnd) {
    mBegin = newBegin;
    mEnd = newEnd;
  }

  public List<ByteBuf> getAllData() {
    return mData;
  }


  /**
   * Needed by hash set judging two element equals
   */
  @Override
  public int hashCode() {
    return (int)((this.mEnd * 31 + this.mBegin) * 31 + this.mFileId ) * 31;
  }


  /**
   * Needed by hash set judging two element equals
   */
  @Override
  public boolean equals(Object obj) {
    if(obj instanceof CacheInternalUnit) {
      CacheInternalUnit tobj = (CacheInternalUnit)obj;
      return this.getFileId() == tobj.getFileId() &&
             this.mBegin == tobj.getBegin() && this.mEnd == tobj.getEnd() ;
    }
    return false;
  }


  public int compareTo(CacheInternalUnit node) {
  	if(node.getBegin() >= this.mEnd) {
  		return -1;
		}
		else if(node.getEnd() <= this.mBegin) {
			return 0;
		}
		return 0;
  }

  /**
   * @return true if no data ref by other cache unit.
   */
  public boolean clearData() {
    //this.before = this.after = null;
    boolean res = true;
    if(mData != null) {
      for (ByteBuf buf : mData) {
        if(!buf.release() ) {
          res = false;
        }
      }
    }
    return res;
    //mData = null;
  }
  private int deletePart(long begin, long newbegin, int lastRemain) {
    int i = 0;
    if(lastRemain > 0 && lastRemain <= newbegin - begin) {
      begin += lastRemain;
      mData.remove(0);
    } else if(lastRemain > newbegin -begin) {
      return lastRemain -(int) (newbegin - begin);
    }
    while (newbegin - begin > 0) {
      ByteBuf buf = mData.get(i);
      begin += buf.capacity();
      if(begin > newbegin) {
        //buf.readerIndex(buf.capacity() - (int)(begin - newbegin));
        //ByteBuf newBuf = buf.slice();
        //buf.release();
        //mData.add(0, newBuf);
        return (int)(begin - newbegin);
      } else {
        buf.release();
        mData.remove(i);
      }
    }
    return 0;
  }

  public void printtest(){
    for(ByteBuf buf : mData) {
      for(int i = 0 ;i < buf.capacity() ; i ++) {
        System.out.print(buf.getByte(i) + " ");
      }
      System.out.println();
    }
  }

  private int generateSubCacheUnit( long newBegin, long newEnd, int lastRemain
	, LinkedFileBucket bucket) {
    CacheInternalUnit newUnit = new CacheInternalUnit(newBegin, newEnd, mFileId);
    try {
      long start = newBegin;
      if (lastRemain > 0) {
        if (lastRemain <= newEnd - newBegin) {
          ByteBuf buf = mData.get(0);
          buf.readerIndex(buf.capacity() - lastRemain);
          ByteBuf newBuf = buf.slice();
          //buf.release();
          mData.remove(0);
          newUnit.mData.add(newBuf);
          start += lastRemain;
        } else {
          ByteBuf buf = mData.get(0);
          ByteBuf newBuf = buf.slice(buf.capacity() - lastRemain, (int) (newEnd - newBegin));
          newUnit.mData.add(newBuf);
          return lastRemain - (int) (newEnd - newBegin);
        }
      }
      while (start < newEnd) {

        ByteBuf tmp = mData.get(0);
        int len = tmp.capacity();
        if (start + len <= newEnd) {
          newUnit.mData.add(tmp);
          start += len;
          mData.remove(0);
        } else {
          ByteBuf newBuf = tmp.slice(0, (int) (newEnd - start));
          newUnit.mData.add(newBuf);
          return len - (int) (newEnd - start);
        }
      }
      return 0;
    } finally {

      this.before.after = newUnit;
      newUnit.before = this.before;
      newUnit.after = this;
      this.before = newUnit;
      //System.out.println("|| " + newUnit.toString());
      //newUnit.printtest();
      //TODO add it to RBTree

			bucket.add(newUnit);
    }
  }

  public void split(Queue<Pair<Long, Long>> tmpQueue, LinkedFileBucket bucket) {
		mDeleteCacheSize = 0;
    long lastEnd = mBegin;
    int lastByteRemain = 0;

    while(!tmpQueue.isEmpty()){
      Pair<Long, Long> currentPart = tmpQueue.poll();
      long begin = currentPart.getKey();
      long end = currentPart.getValue();
      if(begin == mBegin && end == mEnd) return;
      if(begin < 0) continue;
      if (begin != lastEnd) {
        lastByteRemain = deletePart(lastEnd, begin, lastByteRemain);
        mDeleteCacheSize += (begin - lastEnd);
        if(ClientCacheContext.INSTANCE.mUseGhostCache && lastEnd >= 0 && begin > lastEnd) {
					ClientCacheContext.INSTANCE.getGhostCache().add(new BaseCacheUnit(lastEnd, begin, mFileId));
				}
      }

      lastByteRemain = generateSubCacheUnit(begin, end, lastByteRemain, bucket);

      lastEnd = end;
    }
    if(lastEnd != mEnd) {
      deletePart(lastEnd, mEnd, lastByteRemain);
      mDeleteCacheSize += (mEnd - lastEnd);
			if(ClientCacheContext.INSTANCE.mUseGhostCache && lastEnd >= 0 && mEnd > lastEnd) {
				ClientCacheContext.INSTANCE.getGhostCache().add(new BaseCacheUnit(lastEnd, mEnd, mFileId));
			}
    }
  }

  public void clearTreeIndex() {
    this.left = this.right = this.parent = null;
  }
}
