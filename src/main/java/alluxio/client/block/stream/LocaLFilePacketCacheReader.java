package alluxio.client.block.stream;

import alluxio.client.file.FileCache;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.worker.block.io.LocalFileBlockReader;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 *  LocalFilePacketReader从PacketCache中读取文件，和alluxio client端解耦
 */
public class LocaLFilePacketCacheReader implements PacketReader {
  /** The file reader to read a local block. */
  private final LocalFileBlockReader mReader;
  private final PacketCache mPacketCache;
  private long mPos;
  private final long mEnd;
  private final long mPacketSize;

  private boolean mClosed;

  /**
   * Creates an instance of {@link LocalFilePacketReader}.
   *
   * @param path the block path
   * @param offset the offset
   * @param len the length to read
   * @param packetSize the packet size
   */
  private LocaLFilePacketCacheReader(String path, long offset, long len, long packetSize
          ,PacketCache packetCache) throws IOException {
    mReader = new LocalFileBlockReader(path);
    Preconditions.checkArgument(packetSize > 0);
    mPos = offset;
    mEnd = Math.min(mReader.getLength(), offset + len);
    mPacketSize = packetSize;//所以这个变量是没有用的，或者使用这个变量，而不要使用自己的常量
    mPacketCache = packetCache;
  }

  //实际改变了readPacket的语义，因为一次调用就把整个需要的都读完了
  @Override
  public DataBuffer readPacket() throws IOException {//这些都是设置的64KB的buffer，涉及到buffer的拼接问题
    if (mPos >= mEnd) {
      return null;
    }
    //应该不会有这么大的block吧
    int startIndex = (int)(mPos/ FileCache.INSTANCE.PACKET_SIZE);
    int endIndex = (int)(mEnd/FileCache.INSTANCE.PACKET_SIZE);
    ByteBuffer [] buffers = new ByteBuffer[endIndex - startIndex + 1];
    for (int i = startIndex; i <= endIndex; i++) {
      buffers[i] = mPacketCache.getPacket(i);
    }
    for (int i = startIndex; i <= endIndex ; i++) {
      if (buffers[i] == null) {
        long currentOffset = i * FileCache.INSTANCE.PACKET_SIZE;
        ByteBuffer buffer = mReader.read(currentOffset
                , Math.min(FileCache.INSTANCE.PACKET_SIZE, mEnd - currentOffset));
        mPacketCache.add(i, buffer);
      }
    }
    int startOffset = (int)(mPos % FileCache.INSTANCE.PACKET_SIZE);
    int endOffset = (int)(mEnd % FileCache.INSTANCE.PACKET_SIZE);
    DataBuffer dataBuffer = new CompositeDataBuffer(buffers, startOffset, endOffset, mEnd - mPos);
    mPos += dataBuffer.getLength();
    return dataBuffer;
  }

  @Override
  public long pos() {
    return mPos;
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    mClosed = true;
    mReader.close();
  }
}
