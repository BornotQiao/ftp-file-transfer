package com.mobivans.hdfs;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Properties;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.mobivans.ftp.FtpUtils;

public class HdfsUtils {
	
	private static Logger logger = Logger.getLogger(HdfsUtils.class);
//	private static Logger logger_sucs = Logger.getLogger("success_logger");
	private static Logger logger_fail = Logger.getLogger("failure_logger");
	
	//hdfs根路径
	private static String hdfs;
	private static String logServer;
	
	static{
		//在本地windows下跑需要的属性
//		System.setProperty("hadoop.home.dir", "E:/hadoop/hadoop-2.6.0");
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		//ftp连接信息
		Properties prop = new Properties();
		//读取ftp配置文件
		InputStream stream = FtpUtils.class.getResourceAsStream("/hdfs_config.properties");
		try {
			//加载配置文件
			prop.load(stream);
			hdfs = prop.getProperty("hdfs");
			logServer = prop.getProperty("logServer");
			//关闭�?
			stream.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}
	
	/**
	 * 获取FileSystem对象实例
	 * @param isRoot 是否需要root权限
	 * @return
	 */
	private static FileSystem getFS(boolean isRoot){
		Configuration conf = new Configuration();
		FileSystem fileSystem = null;
		try {
			if(isRoot){
				fileSystem = FileSystem.get(new URI(hdfs), conf, "root");
			}
			else
				fileSystem = FileSystem.get(new URI(hdfs), conf);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		return fileSystem;
	}
	
	/**
	 * 关闭FileSystem对象实例链接
	 * @param fileSystem
	 */
	private static void closeFS(FileSystem fileSystem){
		try {
			fileSystem.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 接收文件流，写出到HDFS
	 * @param path
	 * @param in
	 */
	public static void createHdfsFile(String remoteFile){
		//获取连接
		FileSystem fs = getFS(true);
		//解压缩输入流
		GZIPInputStream gzIn = null;
		//hdfs输出流
		FSDataOutputStream fsOutput = null;
		try {
			//存储位置
			String gzFile = hdfs + logServer + remoteFile;
//			String unGzFile = hdfs+logServer+remoteFile.substring(0, remoteFile.lastIndexOf("."));
			//获取ftp压缩文件流
			gzIn = new GZIPInputStream(FtpUtils.getFileStream(remoteFile));
			//创建hdfs文件
//			fsOutput = fs.create(new Path(unGzFile), true);//解压成文本文件存储
			fsOutput = fs.create(new Path(gzFile), true);//不解压，以gz存储
			
			//解压之后，写出到输出流
			IOUtils.copy(gzIn, fsOutput);
			//记录相关日志
			logger.info(remoteFile + " >>> " + gzFile);
		} catch (IOException e2) {
			//下载失败
			logger.error("Failed to Upload " + remoteFile + " to HDFS! Waiting for reupload!");
			//下载失败的记录
			logger_fail.info(remoteFile);
		} finally{
			//关闭流 
			if (gzIn != null) {
				try {
					gzIn.close();
				} catch (IOException e1) {
					e1.printStackTrace();
				} 
			}
			if (fsOutput != null) {
				try {
					fsOutput.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			closeFS(fs);
		}
	}
}
