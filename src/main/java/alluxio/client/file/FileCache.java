package alluxio.client.file;

import alluxio.PropertyKey;
import alluxio.client.Configuration;
import alluxio.client.Constants;
import alluxio.client.block.stream.BlockCache;
import alluxio.client.block.stream.PacketCache;

import alluxio.util.FormatUtils;
import io.netty.util.internal.chmv8.ConcurrentHashMapV8;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 因为add方法是放在BaseFileSystem里的
 */
public enum FileCache {
  INSTANCE;
  //客户端一个packet不会超过2GB吧
  public final int PACKET_SIZE =
          (int)alluxio.Configuration.getBytes(PropertyKey.USER_LOCAL_READER_PACKET_SIZE_BYTES);
  private final int MAX_SLOTS =
          (int)(FormatUtils.parseSpaceSize(Configuration.INSTANCE.getString(Constants.CACHE_PACKET_LIMIT)
          )  /PACKET_SIZE);
  private static final Boolean UNUSED_MAP_VALUE = true;
  private final Map<Long, BlockCache> mPool = new HashMap<>();
  //(todo)为了赶紧测试，这里先用LinkedHashMap实现一下，具体这里怎么写接口，后面再考虑
  private final SimpleCache<PacketIndex, Boolean> mCache = new LRUCache<>(
          MAX_SLOTS, 0.75f, true, MAX_SLOTS);

  public void add(long fileId, BlockCache blockCache) {
    mPool.put(fileId, blockCache);
  }

  public BlockCache getBlockCache(long fileId) {
    BlockCache blockCache = mPool.get(fileId);
    if (blockCache == null) {
      blockCache = new BlockCache(this, fileId);
      mPool.put(fileId, blockCache);
    }
    return blockCache;
  }

  public void remove(long fileId) { mPool.remove(fileId); }

  public long size () {
    int totalPackets = 0;
    for (BlockCache blockCache:mPool.values()) {
      totalPackets += blockCache.size();
    }
    return totalPackets * PACKET_SIZE;
  }

  //
  public void acquireSlot(long fileId, long blockId, int packetIndex) {
    PacketIndex toAddPacket = new PacketIndex(fileId, blockId, packetIndex);
    PacketIndex removedPacket = mCache.add(toAddPacket, UNUSED_MAP_VALUE);
    if (removedPacket != null) {//indicating a packet has to be removed from the cache
      BlockCache targetBlockCache = getBlockCache(removedPacket.getFileId());
      PacketCache targetPacketCache = targetBlockCache.getPacketCache(removedPacket.getBlockId());
      targetPacketCache.remove(removedPacket.getPacketIndex());
      if (targetPacketCache.size() == 0) {
        targetBlockCache.remove(removedPacket.getBlockId());
      }
      if (targetBlockCache.size() == 0) {
        remove(removedPacket.getFileId());
      }
    }
  }

  static class PacketIndex {
    private final long mFileId;
    private final long mBlockId;
    private final int mPacketIndex;

    public PacketIndex(long fileId, long blockId, int packetIndex) {
      this.mFileId = fileId;
      this.mBlockId = blockId;
      this.mPacketIndex = packetIndex;
    }

    public long getFileId() {
      return mFileId;
    }

    public long getBlockId() {
      return mBlockId;
    }

    public int getPacketIndex() {
      return mPacketIndex;
    }
  }
}