package alluxio.client.file.cache;

import alluxio.AlluxioURI;
import alluxio.client.block.stream.BlockInStream;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.GetStatusOptions;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.NotFoundException;
import alluxio.exception.status.UnavailableException;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.proto.journal.Block;
import alluxio.wire.BlockInfo;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableBiMap;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public final class MetedataCache {

  private BiMap<AlluxioURI, URIStatus> mURISet = HashBiMap.create();
  private Map<Long, URIStatus> mFileMetedataSet = new HashMap<>();
  private FileSystemContext mFileSystemContext = FileSystemContext.INSTANCE;
  private final long CHECKER_THREAD_INTERVAL = 1000;
  private ClientCacheContext mContext = ClientCacheContext.INSTANCE;
  public static Map<FileInStream, Map<Long, BlockInStream>> mBlockStreamCache
    = new HashMap<>();
  public HashMap<Long, Map<Long, DataBuffer>> tmpCache = new HashMap<>();



  public MetedataCache() {
		//mContext.COMPUTE_POOL.submit(new ExistChecker());
	}

	public AlluxioURI getUri(long fileId) {
    return mURISet.inverse().get(mFileMetedataSet.get(fileId));
  }

	public URIStatus getStatus(AlluxioURI uri) {
		return mURISet.get(uri);
	}

	public URIStatus getStatus(long fileId) {
    return mFileMetedataSet.get(fileId);
  }
	public boolean cached(AlluxioURI uri) {
		return mURISet.containsKey(uri);
	}

	public void cacheMetedata(AlluxioURI uri, URIStatus stus) {
		mURISet.put(uri, stus);
    mFileMetedataSet.put(stus.getFileId(), stus);
	}

	private boolean isExist(AlluxioURI path)
		throws IOException, AlluxioException {
		{
			FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
			try {
				// TODO(calvin): Make this more efficient
				masterClient.getStatus(path, GetStatusOptions.defaults());
				return true;
			} catch (NotFoundException e) {
				return false;
			} catch (UnavailableException e) {
				throw e;
			} catch (AlluxioStatusException e) {
				throw e.toAlluxioException();
			} finally {
				mFileSystemContext.releaseMasterClient(masterClient);
			}
		}
	}

	class ExistChecker implements Runnable {
		@Override
		public void run() {
			while (true) {
				try {
					Set<AlluxioURI> needRemove = new HashSet<>();
					for (AlluxioURI uri : mURISet.keySet()) {
						if (!isExist(uri)) {
							needRemove.add(uri);
						}
					}
					for (AlluxioURI uri : needRemove) {
						mContext.removeFile(mURISet.get(uri).getFileId());
					}
					mURISet.remove(needRemove);
					Thread.sleep(CHECKER_THREAD_INTERVAL);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

		}
	}
}
