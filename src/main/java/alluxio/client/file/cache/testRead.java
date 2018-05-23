package alluxio.client.file.cache;

import alluxio.AlluxioURI;
import alluxio.Client;
import alluxio.client.file.*;
import org.apache.commons.lang3.RandomUtils;
import sun.awt.windows.ThemeReader;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.*;

public class testRead {
	public static List<Long> beginList = new ArrayList<>();
	public static boolean isWrite = false;
	public static void writeToAlluxio(String s) throws Exception {
		AlluxioURI uri = new AlluxioURI("/testWriteBig");
		FileSystem fs = FileSystem.Factory.get();
		if (fs.exists(uri)) {
			fs.delete(uri);
		}
		FileOutStream out = fs.createFile(uri);
		File f = new File(s);
		BufferedInputStream in = new BufferedInputStream(new FileInputStream(f));
		byte[] b = new byte[1024 * 1024];
		int len = 0;
		long readLen = 0;
		while ((len = in.read(b)) > 0 || readLen < 1024 * 1024 * 1024) {
			out.write(b, 0, len);
			readLen += len;
		}
		out.close();
	}

	public static void readOrigin() throws Exception {
		long begin = new Date().getTime();
		AlluxioURI uri = new AlluxioURI("/testWriteBig");
		FileSystem fs = FileSystem.Factory.get();
		FileInStream in = fs.openFile(uri);
		byte[] b = new byte[1024 * 1024];
		int read;
		int l = 0;
		while ((read = in.positionedRead(l, b, 0, b.length)) != -1) {
			l += read;
		}
		long end = System.currentTimeMillis();
		long time = end - begin;
		System.out.println(time);

	}

	public static void positionReadTest() throws Exception {
		ClientCacheContext.INSTANCE.searchTime = 0;
		CacheManager.mReadTime = 0;
		long begin = System.currentTimeMillis();
		AlluxioURI uri = new AlluxioURI("/testWriteBig");
		FileSystem fs = FileSystem.Factory.get(true);
		FileInStream in = fs.openFile(uri);
		long fileLength = fs.getStatus(uri).getLength();
		int bufferLenth = 1024 * 1024;
		byte[] b = new byte[bufferLenth];
		long beginMax = fileLength - bufferLenth;
		System.out.println("read begin" + Thread.currentThread().getId());
		if(beginList.size() == 0) {
			for (int i = 0; i < 1000; i++) {
				long readBegin = RandomUtils.nextLong(0, beginMax);
				beginList.add(readBegin);
				in.positionedRead(readBegin, b, 0, bufferLenth);
			}
		} else {
			for (int i = 0; i < 1000; i++) {
				in.positionedRead(beginList.get(i), b, 0, bufferLenth);
			}
		}

		long end = System.currentTimeMillis();
		long time = end - begin;

		System.out.println(time);
		System.out.println("search : " + (((FileInStreamWithCache)in)
			.mCacheContext.searchTime));
		System.out.println("read : " + ((FileInStreamWithCache)in).mCachePolicy
			.mReadTime);
	}

	public static void readFirstTime(int l) throws Exception {
		long begin = System.currentTimeMillis();
		AlluxioURI uri = new AlluxioURI("/testWriteBig");
		 FileSystem fs = CacheFileSystem.get();
		//FileSystem fs = FileSystem.Factory.get(true);
		FileInStream in = fs.openFile(uri);
		//	((FileInStreamWithCache)in).mCachePolicy.mReadTime = 0;
		//	ClientCacheContext.INSTANCE.readTime = 0;

		byte[] b = new byte[l];
		int read;
		int ll = 0;
		System.out.println("read begin" + Thread.currentThread().getId());
		while ((read = in.positionedRead(ll, b, 0, b.length)) != -1) {
			ll += read;
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
		  System.out.println("search : " + (((FileInStreamWithCache)in)
				 .mCacheContext.searchTime));
			System.out.println("read : " + ((FileInStreamWithCache)in).mCachePolicy
				.mReadTime);
	}




	public static void main(String[] args) throws Exception {
		//	readFirst
		//if(!isWrite) {
		//	writeToAlluxio("/usr/local/test.gz");
		//  isWrite = true;
		//}

		positionReadTest();
		positionReadTest();

		System.out.println("finish===================");
	}
}