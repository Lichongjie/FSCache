package alluxio.client.file.cache;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.Date;

public class testRead {
  public static void writeToAlluxio(String s) throws Exception {
    AlluxioURI uri = new AlluxioURI("/testWriteBig");
    FileSystem fs = FileSystem.Factory.get();
    if(fs.exists(uri)) {
    	fs.delete(uri);
		}
    FileOutStream out = fs.createFile(uri);
    File f = new File(s);
    BufferedInputStream in = new BufferedInputStream(new FileInputStream(f));
    byte [] b = new byte[1024 * 1024];
    int len = 0;
    while((len = in.read(b)) > 0) {
      out.write(b, 0, len);
    }
    out.close();
  }

  public static void readOrigin() throws Exception {
    long begin = new Date().getTime();
    AlluxioURI uri = new AlluxioURI("/testWriteBig");
    FileSystem fs = FileSystem.Factory.get();
    FileInStream in = fs.openFile(uri);
    byte[] b = new byte[1024  *1024];
    int read;
    while ((read = in.read(b))!= -1) {
		}
		long end = System.currentTimeMillis();
    long time = end - begin;
    System.out.println(time);

  }

  public static void readFirstTime(int l) throws Exception {

    long begin = System.currentTimeMillis();
    AlluxioURI uri = new AlluxioURI("/testWriteBig");
    FileSystem fs = CacheFileSystem.get();
    FileInStream in = fs.openFile(uri);
	//	((FileInStreamWithCache)in).mCachePolicy.mReadTime = 0;
	//	ClientCacheContext.INSTANCE.readTime = 0;
		byte[] b = new byte[l];
		int read;
		while ((read = in.read(b))!= -1) {
		}
		/*
		in = fs.openFile(uri);
		FileInStreamWithCache in2 = (FileInStreamWithCache)in;
		in2.mCachePolicy.clearInputSpace();
	  //b = new byte[2000];
		while ((read = in.read(b))!= -1) {
		}*/

		long end = System.currentTimeMillis();
		long time = end - begin;
		System.out.println(time);
     System.out.println("search : " + (((FileInStreamWithCache)in).mCacheContext.searchTime));
    System.out.println("insert : " + ((FileInStreamWithCache)in).mCachePolicy.mInsertTime);
		System.out.println("read : " + ((FileInStreamWithCache)in).mCachePolicy.mReadTime);
  }
  
  public static void main(String[] args) throws Exception {
	  //	readFirst
	  //	writeToAlluxio("/usr/local/test.gz");
		readFirstTime(1024 * 1024);
		readFirstTime(1024 * 1024 );
		readFirstTime(1024 * 1024 );
		readFirstTime(1024 * 1024 );
		readFirstTime(1024 * 1024 );
	}
}

