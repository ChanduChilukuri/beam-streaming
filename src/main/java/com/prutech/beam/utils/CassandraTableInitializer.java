package com.prutech.beam.utils;

import com.datastax.oss.driver.api.core.CqlSession;

/**
 * this class is for just testing purpose and for creating tables if they didnt
 * created already
 * 
 * @author chandu.ch
 *
 */
public class CassandraTableInitializer {

	public static void createTablesIfNotExist(CqlSession cassandraSession) {

		String violatedEvents = "CREATE TABLE IF NOT EXISTS violation_Events (" + "id UUID PRIMARY KEY,"
				+ "driverId INT," + "truckId INT," + "eventTime TIMESTAMP," + "eventType TEXT," + "latitude DOUBLE,"
				+ "longitude DOUBLE," + "driverName TEXT," + "routeId INT," + "routeName TEXT" + ");";

		String violation_count = "CREATE TABLE IF NOT EXISTS driver_violation_count (" + "driverId INT PRIMARY KEY,"
				+ "incidentRunningTotal COUNTER" + ");";

		String allEvents = "CREATE TABLE IF NOT EXISTS all_driver_events (" + "id UUID PRIMARY KEY," + "driverId INT,"
				+ "truckId INT," + "eventTime TIMESTAMP," + "eventType TEXT," + "latitude DOUBLE," + "longitude DOUBLE,"
				+ "driverName TEXT," + "routeId INT," + "routeName TEXT" + ");";

		// Execute the DDL statements
		cassandraSession.execute(violatedEvents);
		cassandraSession.execute(violation_count);
		cassandraSession.execute(allEvents);
	}
}
