package com.mfniu.example;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

/*
 * 
 * http://www.cloudera.com/documentation/enterprise/latest/topics/cdh_vd_cdh5_maven_repo.html#concept_jhf_dcz_bs_unique_2
 * 
 */
public class HdfsApp {

	private static Configuration conf = new Configuration();
	private static FileSystem fs;
	private static DistributedFileSystem hdfs;

	public static void main(String[] args) {

		try {

			System.setProperty("HADOOP_USER_NAME", "root");

			fs = FileSystem.get(conf);

			hdfs = (DistributedFileSystem) fs;

		} catch (Exception e) {

			e.printStackTrace();

		}

		String file = "/mfniu/room/chat/1.log";

		// createFile(file);

		// deleteFile(file);

		/*
		 * 
		 * if (checkFileExist(file) == false) { createFile(file); }
		 * 
		 * try {
		 * 
		 * appendToHdfs(file, "0\n");
		 * 
		 * } catch (IOException e) { e.printStackTrace(); }
		 */

	}

	public static void createFile(String path) {

		try {

			Path f = new Path(path);

			FSDataOutputStream os = fs.create(f, true);

			Writer out = new OutputStreamWriter(os, "utf-8");

			out.close();

			os.close();

		} catch (Exception e) {

			e.printStackTrace();

		}

	}

	/**
	 * 判断文件是否存在
	 */
	public static boolean checkFileExist(String path) {

		boolean exist = false;

		try {

			Path f = new Path(path);

			exist = fs.exists(f);

		} catch (Exception e) {

			e.printStackTrace();

		}

		return exist;
	}

	public static void deleteFile(String path) {

		Path f = new Path(path);

		try {

			boolean isDeleted = hdfs.delete(f, false);

			System.out.println("Delete success");

		} catch (IOException e) {

			e.printStackTrace();
		}

	}

	public static void appendToHdfs(String path, String content) throws IOException {

		FileSystem fs = FileSystem.get(URI.create(path), conf);

		FSDataOutputStream out = fs.append(new Path(path));

		int readLen = content.getBytes().length;

		out.write(content.getBytes(), 0, readLen);

		out.close();

		fs.close();

	}

}
