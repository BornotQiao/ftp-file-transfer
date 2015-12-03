package com.mobivans.main;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import com.mobivans.ftp.FtpUtils;

public class Main {

	private static Logger logger = Logger.getLogger(Main.class);

	private static String done_log = "";
	private static String fail_log = "";
	
	static{
		//ftp连接信息
		Properties prop = new Properties();
		//读取ftp配置文件
		InputStream stream = FtpUtils.class.getResourceAsStream("/log4j.properties");
		try {
			//加载配置文件
			prop.load(stream);
			
			done_log = prop.getProperty("log4j.appender.done.File");
			fail_log = prop.getProperty("log4j.appender.fail.File");
			
			//关闭�?
			stream.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

	public static void main(String[] args) {
		logger.info("Download task is starting...");
		try {
			//开始下载
			downloadStart(done_log, fail_log);
		} catch (IOException e) {
			e.printStackTrace();
		} 
	}
	
	/**
	 * 开始下载
	 * @param doneLog
	 * @param failLog
	 * @throws IOException
	 */
	public static void downloadStart(String doneLog, String failLog) throws IOException{
		File doneLogFile = new File(doneLog);
		//判断记录文件是否存在
		if(doneLogFile.length() != 0){
			//判断最后5条记录中最后一条是不是该文件夹下的最后一条记录（由于是多线程下载，所以最后一条记录不一定是最后一条记录）
			String lastLine = computeLastLine(doneLog);
			//最后一个下载成功的文件
			String wholePath = lastLine.split(" ")[2];
			String dir = wholePath.substring(0, wholePath.lastIndexOf("/"));
			String file= wholePath.substring(wholePath.lastIndexOf("/")+1, wholePath.length()).trim();
			//当前日期文件夹下所有文件
			List<String> files = FtpUtils.listFiles(dir);
			//判断当前文件是否是该文件夹最后一个文件
			if((files.size()-1) != files.indexOf(file)){//不是该文件夹下最后一个文件
				//继续下载没下完文件的文件夹
				FtpUtils.downloadRestFiles(dir, file);
			}
			//下载后续的文件夹
			String beginDir = Long.parseLong(dir.substring(1, dir.length()))+1+"";
			FtpUtils.downloadSpecifiedDirectories("/", beginDir);//downloadDirectory("/", beginDir);
		} else{
			//记录日志不存在，就从第一个开始下载
			FtpUtils.downloadAllDirectories();
//			FtpUtils.downloadDefaultDirectories();
		}
		//重新下载失败日志记录的文件
		File failLogFile = new File(failLog);
		if(failLogFile.length() > 0){
			downloadFailLog(failLog);
		}
	}
	
	/**
	 * 下载失败日志记录的文件
	 * @param down
	 * @param failLog
	 */
	public static void downloadFailLog(String failLog){
		logger.info("Starting to redownload the fail.log's file.");
		//读取失败记录
		List<String> lines = readLineToList(failLog);//"log/fail.log"
		//清空失败记录
		clearFileContent(failLog);//"log/fail.log"
		Map<String, String> fileInfo = new HashMap<>();
		//遍历失败记录，放入Map
		for(String line : lines){
			//全路径
			String wholePath = line.split(" ")[2];
			//文件夹
			String dir = wholePath.substring(0, wholePath.lastIndexOf("/"));
			//文件
			String file = wholePath.substring(wholePath.lastIndexOf("/")+1, wholePath.length());
			//下载失败的文件重新下载
			fileInfo.put(dir, file);
		}
		//重新下载失败列表中的文件
		FtpUtils.downloadSpecifiedFiles(fileInfo);
		//如果失败日志仍旧有记录，递归下载
		if(!readLineToList(failLog).isEmpty()){
			downloadFailLog(failLog);
		}
	}
	
	/**
	 * 判读最后5行中的最后一条记录
	 * @param doneLog
	 * @return
	 */
	private static String computeLastLine(String doneLog){
		//所有行
		List<String> lines = readLineToList(doneLog);
		//存放最后5条记录文件名的时间值
		List<Long> last5LinesTimeValue = new ArrayList<>();
		if(lines.size() >= 5){
			for (int i = lines.size()-5; i < lines.size(); i++) {
				last5LinesTimeValue.add(cutTimeOfLine(lines.get(i)));
			}
		} else{
			for(int i=lines.size()-1; i>=0; i--){
				last5LinesTimeValue.add(cutTimeOfLine(lines.get(i)));
			}
		}
		//排序
		Collections.sort(last5LinesTimeValue);
		//下载成功的最后一个文件
		String lastLine = "";
		//返回最大值，即最后一条记录
		for (String line : lines) {
			if(line.contains(last5LinesTimeValue.get(last5LinesTimeValue.size()-1).toString())){
				lastLine = line;
			}
		}
		return lastLine;
	}
	/**
	 * 截取一行的文件名中的时间值
	 * @param line
	 * @return
	 */
	private static Long cutTimeOfLine(String line){
		String longValue = line.split(" ")[2].split("/")[2].split("_")[0];
		return Long.parseLong(longValue);
	}
	
	
	/**
	 * 读取文件每一行，放入list
	 * @param file
	 * @return
	 */
	private static List<String> readLineToList(String file){
		List<String> lines = null;
		File f = new File(file);
		if (!f.exists()) {
			logger.info("<"+file+"> is not exist");
			return lines;
		}
		try {
			lines = IOUtils.readLines(new FileInputStream(f));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return lines;
	}
	
	/**
	 * 读取文件最后一行
	 * @param file
	 * @return 返回最后一行内容
	 */
	@SuppressWarnings("unused")
	private static String readLastLine(File file) {
		if (!file.exists() || file.isDirectory() || !file.canRead()) {
			return null;
		}
		RandomAccessFile raf = null;
		try {
			raf = new RandomAccessFile(file, "r");
			long len = raf.length();
			if (len == 0L) {
				return "";
			} else {
				long pos = len - 1;
				while (pos > 0) {
					pos--;
					raf.seek(pos);
					if (raf.readByte() == '\n') {
						break;
					}
				}
				if (pos == 0) {
					raf.seek(0);
				}
				byte[] bytes = new byte[(int) (len - pos)];
				raf.read(bytes);
				return new String(bytes);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (raf != null) {
				try {
					raf.close();
				} catch (Exception e2) {
				}
			}
		}
		return null;
	}
	
	/**
	 * 读取文件内容后，将文件内容清空
	 * @param fileName
	 */
	private static void clearFileContent(String fileName) {
		File file = new File(fileName);
		try {
			if (!file.exists()) {
				file.createNewFile();
			}
			FileWriter fileWriter = new FileWriter(file);
			fileWriter.write("");
			fileWriter.flush();
			fileWriter.close();
			logger.info("File <"+ fileName +"> has already cleared.");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
