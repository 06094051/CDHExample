package com.mfniu.example.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;

public class HbaseApp {

	public static Configuration conf;

	public static void main(String[] args) throws IOException {

		System.setProperty("HADOOP_USER_NAME", "root");

		conf = HBaseConfiguration.create();

		createTable();

	}

	public static void createTable() throws IOException {

		HConnection hConnection = HConnectionManager.createConnection(conf);

		HBaseAdmin admin = (HBaseAdmin) hConnection.getAdmin();

		HTableDescriptor desc = admin.getTableDescriptor(TableName.valueOf("mfniu.spider.article"));

		System.out.println(desc.getOwnerString());

		HTableDescriptor[] descs = admin.listTables();

		for (HTableDescriptor d : descs) {

			System.out.println(d.getTableName().getNameAsString());

		}

	}

}
