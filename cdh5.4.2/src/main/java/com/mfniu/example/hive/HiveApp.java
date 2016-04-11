package com.mfniu.example.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveApp {

	public static void main(String[] args) {

		Connection hiveConnection = getConnection();

		String sqlQuery = "select * from domainname limit 10";

		Query(hiveConnection, sqlQuery);

	}

	private static Connection getConnection() {

		try {
			
			Class.forName("org.apache.hive.jdbc.HiveDriver");

		} catch (ClassNotFoundException e) {

			e.printStackTrace();

		}

		try {

			return DriverManager.getConnection("jdbc:hive2://datacenter1:10000/default");

		} catch (SQLException e) {

			e.printStackTrace();

		}

		return null;

	}

	private static void Query(Connection hiveConnection, String sql) {

		try {

			Statement hiveStatement = hiveConnection.createStatement();

			ResultSet rs = hiveStatement.executeQuery(sql);

			ResultSetMetaData resultSetMetaData = rs.getMetaData();

			int columnCount = resultSetMetaData.getColumnCount();

			while (rs.next()) {

				for (int i = 1; i <= columnCount; i++) {

					System.out.println(rs.getString(i));

				}

				System.out.println();

			}

			rs.close();

		} catch (SQLException e) {

			e.printStackTrace();

		}

	}
}
