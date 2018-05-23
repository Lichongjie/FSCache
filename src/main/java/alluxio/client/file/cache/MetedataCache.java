package alluxio.client.file.cache;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.GetStatusOptions;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.NotFoundException;
import alluxio.exception.status.UnavailableException;

import java.io.IOException;
import java.util.*;

public final class MetedataCache {
	private Map<AlluxioURI, URIStatus> mURISet = new LinkedHashMap<>();
	private FileSystemContext mFileSystemContext = FileSystemContext.INSTANCE;
	private final long CHECKER_THREAD_INTERVAL = 1000;
	private ClientCacheContext mContext = ClientCacheContext.INSTANCE;

	public MetedataCache() {
		//mContext.COMPUTE_POOL.submit(new ExistChecker());
	}

	public URIStatus getStatus(AlluxioURI uri) {
		return mURISet.get(uri);
	}

	public boolean cached(AlluxioURI uri) {
		return mURISet.containsKey(uri);
	}

	public void cacheMetedata(AlluxioURI uri, URIStatus stus) {
		mURISet.put(uri, stus);
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
