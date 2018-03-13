package com.unb.bd.etl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DataBase {

	private Connection conn = null;
	private static DataBase instance = null;
	private static final String url = "jdbc:mysql://localhost/";
	private static final String user_name = "root";
	private static final String password = "";
	private static final String database = "diarias";
	private static final String driver = "com.mysql.jdbc.Driver";

	static public DataBase getInstance() {
		if (instance == null) {
			return new DataBase(url + database, user_name, password);
		}
		return instance;
	}

	private DataBase(String url, String user_name, String password) {
		try {
			Class.forName(driver);
			conn = DriverManager.getConnection(url, user_name, password);
		} catch (SQLException e) {
			System.out.println("Nao foi possivel se conectar ao banco.");
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			System.out.println("Nao foi possivel encontrar o driver indicado.");
			e.printStackTrace();
		}
	}

	public ResultSet executeSqlQuery(String sql) throws SQLException {
		Statement statement = conn.createStatement();
		return statement.executeQuery(sql);
	}

	public void executeSqlUpdateQuery(String sql) throws SQLException {
		Statement statement = conn.createStatement();
		statement.executeUpdate(sql);
	}

	@Override
	protected void finalize() throws Throwable {
		try {
			if (conn != null && !conn.isClosed()) {
				conn.close();
			}
		} catch (SQLException e) {
			System.out.println("Nao foi possivel finalizar a conexao.");
			e.printStackTrace();
		}
	}

	static String formatDateToBank(String dateToFormat) {
		String[] split = dateToFormat.split("/");

		return "'" + split[2] + "-" + split[1] + "-" + split[0] + "'";
	}

	static String formatStringToBank(String s) {
		return "\"" + s + "\"";
	}
}
