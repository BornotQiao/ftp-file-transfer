package com.mobivans.ftp;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.log4j.Logger;

import com.mobivans.hdfs.HdfsUtils;

/**
 * 多线程按目录批量下载
 * 
 * @author Qiao
 * 
 */
public class FtpUtils {

	private static Logger logger = Logger.getLogger(FtpUtils.class);
	private static Logger logger_sucs = Logger.getLogger("success_logger");
	private static Logger logger_fail = Logger.getLogger("failure_logger");
	
	// 本地存储目录
	private static String local_path;
	// 远程下载目录
	private static String remote_path;
	// 用户名
	private static String user;
	// 密码
	private static String pswd;
	// IP
	private static String host;
	// port
	private static int port;
	// /被动模式
	private static boolean isPASV;
	// 默认下载文件夹数
	private static int _num;
	// 是否覆盖
//	private static boolean cover;
	// 线程池线程个数
	private static int threads;

	static{
		//ftp连接信息
		Properties prop = new Properties();
		//读取ftp配置文件
		InputStream stream = FtpUtils.class.getResourceAsStream("/ftp_config.properties");
		try {
			//加载配置文件
			prop.load(stream);
			local_path = prop.getProperty("local_path");//本地存储路径
			remote_path = prop.getProperty("path");//远程路径
			host = prop.getProperty("HOST");
			port = Integer.parseInt(prop.getProperty("PORT"));
			user = prop.getProperty("username");
			pswd = prop.getProperty("password");
			isPASV = Boolean.parseBoolean(prop.getProperty("isPASV"));
//			cover = Boolean.parseBoolean(prop.getProperty("cover"));
			_num = Integer.parseInt(prop.getProperty("_num"));
			threads = Integer.parseInt(prop.getProperty("threads"));
			//关闭�?
			stream.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

	/**
	 * 关闭ftp链接
	 * @param client
	 */
	private static void disConnect(FTPClient client){
		if (client.isConnected()) {
			try {
//				client.logout();
				client.disconnect();
			} catch (IOException e) {
				e.printStackTrace();
			} 
		}
	}

	/**
	 * 获取ftp连接
	 * @return
	 * @throws SocketException
	 * @throws IOException
	 */
	private static FTPClient getConnect() {
		FTPClient ftpClient = new FTPClient();
		try {
			ftpClient.connect(host, port);
			ftpClient.login(user, pswd);// 登录
			ftpClient.setDataTimeout(60000);//传输超时时间
			ftpClient.setConnectTimeout(60000);//连接超时
			if (isPASV) {
				ftpClient.enterLocalPassiveMode(); // 被动模式
			} else {
				ftpClient.enterLocalActiveMode(); // 主动模式
			}
			//切换到根目录
			ftpClient.changeWorkingDirectory("/");
			//设置文件传输方式（默认FTP.ASCII_FILE_TYPE-会损失文件精度）此处设置至关重要！！！！！！！！
			ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
		} catch (SocketException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return ftpClient;
	}

	/**
	 * 下载所有文件夹
	 * @throws IOException
	 */
	public static void downloadAllDirectories() throws IOException{
		//根目录下所有文件夹
		List<String> dirList = listDirs(remote_path);
		//控制下载文件夹个数
		for(int i=0; i < dirList.size(); i++){
			downloadDirctory(remote_path+File.separator+dirList.get(i));
		}
	}
	
	/**
	 * 从根目录第一个文件夹开始下载,下载_num个
	 * @param remotePath
	 * @throws IOException 
	 */
	public static void downloadDefaultDirectories() throws IOException{
		//根目录下所有文件夹
		List<String> dirList = listDirs(remote_path);
		//控制下载文件夹个数
		for(int i=0; i < _num; i++){
			downloadDirctory(remote_path+File.separator+dirList.get(i));
		}
	}
	
	/**
	 * 从指定文件夹开始下载，下载_num个
	 * @param remotePath 根目录
	 * @param beginDir 指定开始下载位置
	 * @throws IOException 
	 */
	public static void downloadSpecifiedDirectories(String remotePath, String beginDir) throws IOException{
		//根目录下所有文件夹
		List<String> dirList = listDirs(remotePath);
		//开始下载文件夹的下标
		int curIndx = dirList.indexOf(beginDir), tmp = curIndx;
		
		//截止下载文件夹的下标
		int toIndex = (tmp + _num) > dirList.size()-1 ? dirList.size()-1 : (tmp + _num);
		
		//从开始下载文件夹下标开始遍历_num个文件夹
		for(; curIndx <= toIndex; curIndx++){
			downloadDirctory(remotePath+File.separator+dirList.get(curIndx));
		}
	}
	
	/**
	 * 下载指定文件
	 * 此处用于下载失败日志中的文件
	 * @param remoteFileString remoteDir, String remoteFile
	 */
	public static void downloadSpecifiedFiles(Map<String, String> fileInfo){
		//创建线程池
		ExecutorService threadPool = Executors.newFixedThreadPool(threads);
		
		Iterator<String> iterator = fileInfo.keySet().iterator();
		while(iterator.hasNext()){
			String dir = iterator.next();
			String file = fileInfo.get(dir);
			
			//下载准备，flag为0的是重新下载失败的记录，为1的是第一次下载
			threadPool.execute(createDownloadThread(dir, file, 0));
		}
		threadPool.shutdown();
	}

	/**
	 * 下载当前文件夹剩余文件
	 * @param remoteDir
	 * @param fileName
	 * @throws IOException
	 */
	public static void downloadRestFiles(String remoteDir, String fileName) throws IOException{
		FTPClient ftpClient = getConnect();
		//当前目录下所有文件
		List<String> fileList = listFiles(remoteDir);
		//创建线程池
		ExecutorService threadPool = Executors.newFixedThreadPool(threads);
		//当前文件夹下除去fileName之前的剩余文件
		for (int i=fileList.indexOf(fileName) + 1; i<fileList.size(); i++) {
			try {
				//线程阻塞
				Thread.sleep(500);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			//第一次下载
			threadPool.execute(createDownloadThread(remoteDir, fileList.get(i), 1));
		}
		threadPool.shutdown();
		disConnect(ftpClient);
	}

	/**
	 * 列出当前文件夹下所有文件
	 * @param remoteDir
	 * @return
	 */
	public static List<String> listFiles(String remoteDir){
		FTPClient ftpClient = getConnect();
		//当前目录下所有文件
		List<String> fileList = new ArrayList<String>();
		FTPFile[] listFiles;
		try {
			listFiles = ftpClient.listFiles(remoteDir);
			for (FTPFile ftpFile : listFiles) {
				fileList.add(ftpFile.getName());
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally{
			disConnect(ftpClient);
		}
		return fileList;
	}

	/**
	 * 列出当前文件夹下所有文件夹
	 * @param parentDir
	 * @return
	 */
	public static List<String> listDirs(String parentDir){
		FTPClient ftpClient = getConnect();
		//当前目录下所有文件
		List<String> dirList = new ArrayList<String>();
		try {
			for(FTPFile dir : ftpClient.listDirectories(parentDir)){
				dirList.add(dir.getName());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return dirList;
	}

	/**
	 * 获取FTP文件流
	 * @param remoteFile
	 * @return
	 */
	public static InputStream getFileStream(String remoteFile){
		InputStream stream = null;
		//获取ftp服务器连接
		FTPClient ftpClient = getConnect();
		try {
			stream = ftpClient.retrieveFileStream(remoteFile);
		} catch (IOException e) {
			e.printStackTrace();
		} finally{
			//关闭ftp连接
			disConnect(ftpClient);
		}
		return stream;
	}

	/**
	 * 下载文件夹
	 * @param remotePath 
	 * @throws IOException 
	 */
	private static void downloadDirctory(String remotePath) throws IOException{
		remotePath = remotePath.replace("\\", "/").replace("//", "/");
		//创建线程池
		ExecutorService threadPool = Executors.newFixedThreadPool(threads);
		//当前文件夹下所有文件
		List<String> fileList = listFiles(remotePath);
		//遍历所有文件，创建下载线程
		for(int i=0; i<fileList.size(); i++){
			//线程阻塞
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			//执行线程任务
			threadPool.execute(createDownloadThread(remotePath, fileList.get(i), 1));
		}
		//关闭线程池
		threadPool.shutdown();
		//死循环：当有线程任务没完成时，等待线程任务完成
		while(true){
			if(threadPool.isTerminated()){
				break;
			} else{
				try {
					Thread.sleep(1000*10);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * 下载前的准备工作
	 * @param remote_path 远程目录
	 * @param file 
	 * @param flag 判断是否是第一次下载该文件。0-重新下载 1-第一次下载
	 */
	private static WorkingThread createDownloadThread(String remote_path, String ftpFile, int flag) {
		// full path on server
		String remote_file = (remote_path + File.separator + ftpFile).replace('\\', '/');
		String local_file = (local_path + remote_path + File.separator + ftpFile).replace("\\", "/");
		//创建下载线程
		WorkingThread thread = new WorkingThread(remote_file, local_file, flag);
		
		return thread;
	}

	/**
	 * 线程下载类
	 * @author Qiao
	 *
	 */
	static class WorkingThread extends Thread {
		//远程文件全路径名称
		private String rem_file;
		//本地文件
//		private String local_file;
		//ftp客户端
		private FTPClient ftpClient;
		//判断是重新下载还是第一次下载，来决定是否记录到done.log。否则失败重新下载的又得重新下载一次
		private int flag;

		public WorkingThread(String rem_file, String local_file, int flag) {
				this.rem_file = rem_file;
//				this.local_file = local_file;
				this.flag = flag;
		}
		/**
		 * 读取ftp文件流，写出到本地
		 * @param remoteFile
		 * @param localFile
		 * @throws IOException
		 */
		@SuppressWarnings("unused")
		private void writeRemoteGzToLocal(String remoteFile, String localFile) throws IOException{
			//获取ftp连接
			ftpClient = FtpUtils.getConnect();
			//创建本地文件目录
			createLocalDirIfNotExists(localFile);
			//指定本地目录文件
			OutputStream out = new BufferedOutputStream(new FileOutputStream(new File(localFile)));
			//写出到本地
			IOUtils.copy(ftpClient.retrieveFileStream(remoteFile), out);
			//刷新
			out.flush();
			//关闭
			out.close();
			//关闭ftp连接
			disConnect(ftpClient);
		}
		
		/**
		 * 解压缩并下载到本地
		 * @param remoteFile
		 * @throws IOException
		 */
		@SuppressWarnings("unused")
		private void decompressRemoteGzToLocal(String remoteFile) {
			//获取ftp连接
			ftpClient = FtpUtils.getConnect();
			//处理解压缩文件名
			String localFile = local_path + remoteFile.substring(0, remoteFile.lastIndexOf("."));
			//创建本地文件目录
			createLocalDirIfNotExists(localFile);
			//解压缩文件流
			GZIPInputStream gzipInput = null;
			//文件输出流
			BufferedOutputStream bufOutput = null;
			try {
				gzipInput = new GZIPInputStream(ftpClient.retrieveFileStream(remoteFile));
				bufOutput = new BufferedOutputStream(new FileOutputStream(localFile));
				IOUtils.copy(gzipInput, bufOutput);
			} catch (IOException e) {
				logger_fail.info(remoteFile);
				logger.info(rem_file+" 解压失败异常：" + e);
			} finally{
				//关闭流
				try {
					gzipInput.close();
					bufOutput.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
				//关闭ftp连接
				disConnect(ftpClient);
			}
		}
		/**
		 * 建立本地文件目录
		 * @param local_file
		 */
		private static void createLocalDirIfNotExists(String local_file) {
			String fs = new File(local_file).getParent();
			File f = new File(fs);
			if (!f.exists()) {
				logger.info("Created local dir: " + f.getAbsolutePath());
				f.mkdirs();
			}
		}
		/**
		 * 任务执行
		 */
		public void run() {
			try {
				//不解压写到本地
//				writeRemoteFileToLocal(rem_file, local_file);
				//解压缩写到本地
//				decompressRemoteGzToLocal(rem_file);
				//解压之后上传到hdfs
				HdfsUtils.createHdfsFile(rem_file);
				//判断是否记录到done.log日志中
				if(flag == 1){
					//记录到下载成功日志
					logger_sucs.info(rem_file);
				}
				//下载到本地的时候，启动。记录到日志
//				logger.info(rem_file + " >>> " + local_file.substring(0, local_file.lastIndexOf(".")));//;
			} catch (NullPointerException e) {
				//记录到下载失败日志，等待重新下载
				logger_fail.info(rem_file);
				logger.info("Connections to FTP server is too much, please reconnect it later.");
				try {
					Thread.sleep(1000*30);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
			} 
		}
	}
}
