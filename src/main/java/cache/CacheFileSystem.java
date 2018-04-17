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

package cache;

import alluxio.AlluxioURI;
import alluxio.client.file.*;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;

import java.io.IOException;

public class CacheFileSystem extends BaseFileSystem {

  protected final ClientCacheContext mCacheContext;

  public static CacheFileSystem get(FileSystemContext context, ClientCacheContext cacheContext) {
    return new CacheFileSystem(context, cacheContext);
  }

  private CacheFileSystem(FileSystemContext context, ClientCacheContext cacheContext) {
    super(context);
    mCacheContext = cacheContext;
  }

  /*
  @Override
  public FileOutStream createFile(AlluxioURI path) throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException {
    return createFile(path, CreateFileOptions.defaults());
  }

  @Override
  public FileOutStream createFile(AlluxioURI path, CreateFileOptions options)
          throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException {
    FileOutStream out = super.createFile(path, options);
    return new FileOutStreamWithCache(out.mUri, out.mOptions, out.mContext, mCacheContext);
  }
  */

  @Override
  public FileInStream openFile(AlluxioURI path) throws FileDoesNotExistException, IOException, AlluxioException {
    return openFile(path, OpenFileOptions.defaults());
  }

  @Override
  public FileInStream openFile(AlluxioURI path, OpenFileOptions options) throws FileDoesNotExistException, IOException, AlluxioException {
    FileInStream in = super.openFile(path, options);
    return new FileInStreamWithCache(in, mCacheContext);
  }



}
