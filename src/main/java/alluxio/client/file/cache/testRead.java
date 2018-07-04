package alluxio.client.file.cache;

import alluxio.AlluxioURI;
import alluxio.Client;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.cache.submodularLib.cacheSet.CacheSet;
import alluxio.client.file.cache.submodularLib.cacheSet.CacheSpaceCalculator;
import org.apache.commons.lang3.RandomUtils;
import sun.awt.windows.ThemeReader;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.*;

public class testRead {
	public static List<Long> beginList = new ArrayList<>();
	public static boolean isWrite = false;
	public static int allInterruptedTime = 0;
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

	public static final int getProcessID() {
		RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
		System.out.println(runtimeMXBean.getName());
		return Integer.valueOf(runtimeMXBean.getName().split("@")[0])
			.intValue();
	}


	public static void getInterruptedTime() {
		int i = getProcessID();
		Runtime run = Runtime.getRuntime();
		try {
			Process process = run.exec("ps -o min_flt,maj_flt " + i);
			InputStream in = process.getInputStream();
			BufferedReader bs = new BufferedReader(new InputStreamReader(in));
			String s = "";
			String res= " ";
			while((s = bs.readLine()) != null) {
				res= s;
			}
			int res1 = Integer.parseInt(res.split("\\s+")[1]);
			System.out.println("interrupted time : " + (res1 - allInterruptedTime));
			allInterruptedTime = res1;
			in.close();
			process.destroy();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void testSpace() {
		CacheSet s = new CacheSet();
		for(int i = 0; i < beginList.size(); i ++) {
			long begin11 = beginList.get(i);
			s.add(new BaseCacheUnit(begin11, begin11 + 1024 * 1024, 1));
		}
		CacheSpaceCalculator c = new CacheSpaceCalculator();
		System.out.println("space "+ c.function(s));
	}


	public static void positionReadTest() throws Exception {
		//ClientCacheContext.INSTANCE.searchTime = 0;
		long begin = System.currentTimeMillis();
		AlluxioURI uri = new AlluxioURI("/testWriteBig");
		FileSystem fs = FileSystem.Factory.get(true);
		FileInStream in = fs.openFile(uri);
		long fileLength = fs.getStatus(uri).getLength();
		int bufferLenth = 1024 * 1024;
		byte[] b = new byte[bufferLenth];
		long beginMax = fileLength - bufferLenth;
		System.out.println("read begin" + Thread.currentThread().getId());
		ClientCacheContext.checkout = 0;
		ClientCacheContext.missSize = 0;
		ClientCacheContext.hitTime = 0;
		if(beginList.size() == 0) {
			for (int i = 0; i < 1024; i++) {
				long readBegin = RandomUtils.nextLong(0, beginMax);
				beginList.add(readBegin);
				in.positionedRead(readBegin, b, 0, bufferLenth);
			}
		} else {
			for (int i = 0; i < 1024; i++) {
				in.positionedRead(beginList.get(i), b, 0, bufferLenth);
			}
		}

		long end = System.currentTimeMillis();
		long time = end - begin;

		System.out.println(time);
		//getInterruptedTime();
    /*
		System.out.println("search : " + (((FileInStreamWithCache)in)
			.mCacheContext.searchTime));
		System.out.println("read : " + ((FileInStreamWithCache)in).mCachePolicy
			.mReadTime);
		System.out.println("break time" + ClientCacheContext.checkout + " hit " +
			"ratio"  +  (1 - ((double)ClientCacheContext.missSize / 1024 / 1024 /
			(double)1024)) +
			" hitTime " +
			ClientCacheContext.hitTime);*/
	}

	public static void readFirstTime(int l) throws Exception {

		long begin = System.currentTimeMillis();
		AlluxioURI uri = new AlluxioURI("/testWriteBig");
		// FileSystem fs = CacheFileSystem.get();
		FileSystem fs = FileSystem.Factory.get(true);
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
	}



	public static void main(String[] args) throws Exception {
		//	readFirst
		//if(!isWrite) {
		//writeToAlluxio("/usr/local/test.gz");
		//  isWrite = true;
		//}
    for(int i = 0 ; i <10 ;i ++) {
			positionReadTest();
		}

		System.out.println("finish===================");
	}
}