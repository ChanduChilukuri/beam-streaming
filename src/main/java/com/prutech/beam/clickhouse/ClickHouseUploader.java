package com.prutech.beam.clickhouse;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import com.prutech.beam.utils.PropertyProvider;

public class ClickHouseUploader {

	private final PropertyProvider propertyProvider;
	private final String clickHouseUrl;
	private final String tableName;

	public ClickHouseUploader(PropertyProvider propertyProvider) {
		this.clickHouseUrl = propertyProvider.getProperty("clickHouseUrl");
		this.tableName = propertyProvider.getProperty("clickHouseTableName");
		this.propertyProvider = propertyProvider;

	}

	public void uploadCsvFile(String tableName, InputStream inputStream) throws SQLException {
		try (Connection connection = DriverManager.getConnection(clickHouseUrl);
				Statement statement = connection.createStatement()) {

			// Create a buffer to read from the input stream
			BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

			// Skip the header row
			reader.readLine();

			// Prepare the query for inserting data
			String query = "INSERT INTO " + tableName + " FORMAT CSV";

			// Read and insert each line of the CSV data
			String line;
			while ((line = reader.readLine()) != null) {
				statement.addBatch(query + "\n" + line);
			}

			// Execute the batch
			statement.executeBatch();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public String uploadcsvquery(/* String data */) {
		String query = "INSERT INTO " + tableName + " FORMAT CSV";
		return query;
	}

	/**
	 * 
	 * @param id
	 * @param name
	 * @param age
	 * @throws SQLException
	 */
	public void uploadToClickHouse(int id, String name, int age) throws SQLException {
		try (Connection connection = DriverManager.getConnection(clickHouseUrl);
				PreparedStatement statement = connection.prepareStatement(getInsertQuery())) {
			statement.setInt(1, id);
			statement.setString(2, name);
			statement.setInt(3, age);
			statement.executeUpdate();
		}
	}

	private String getInsertQuery() {
		// System.out.println(tableName);
		return "INSERT INTO " + tableName + " (id, name, age) VALUES (?, ?, ?)";
	}

	/**
	 * creating table in click house
	 * 
	 * @throws SQLException
	 */
	public void createTableWithPartitions() throws SQLException {
		try (Connection connection = DriverManager.getConnection(clickHouseUrl);
				PreparedStatement statement = connection.prepareStatement(tablecreate())) {
			statement.executeUpdate();
		}
	}

	private String tablecreate() {
		String query = "CREATE TABLE IF NOT EXISTS" + " " + "visits" + "(VisitDate Date,Hour UInt8,ClientID UUID)"
				+ "ENGINE = MergeTree() PARTITION BY toYYYYMM(VisitDate) ORDER BY Hour";
		return query;
	}

	/**
	 * uploading partitioned data to a partition table
	 * 
	 * @param id
	 * @param name
	 * @param age
	 * @param date
	 * @param hours
	 * @throws SQLException
	 */
	public void uploadPartitionedData(int id, String name, int age, String date, int hours) throws SQLException {
		try (Connection connection = DriverManager.getConnection(clickHouseUrl);
				PreparedStatement statement = connection.prepareStatement(getInsertPartitionedQuery())) {
			statement.setInt(1, id);
			statement.setString(2, name);
			statement.setInt(3, age);
			statement.setString(4, date);
			statement.setInt(5, hours);
			statement.executeUpdate();
		}
	}

	private String getInsertPartitionedQuery() {
		return "INSERT INTO " + "events" + " (id, name, age, date, hours) VALUES (?, ?, ?, ?, ?)";
	}
}
