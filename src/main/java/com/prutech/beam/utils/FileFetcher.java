package com.prutech.beam.utils;

import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.prutech.beam.clickhouse.ClickHouseUploader;

public class FileFetcher {

	private final PropertyProvider propertyProvider;
	private final Storage storage;
	private final String bucketName;
	private final String objectNamePrefix;
	private final String clickHouseUrl;
	private final String clickHouseTableName;

	/**
	 * constructor for class
	 * 
	 * @param propertyProvider
	 * @param storage
	 */
	public FileFetcher(PropertyProvider propertyProvider, Storage storage) {
		this.propertyProvider = propertyProvider;
		this.storage = storage;
		this.bucketName = propertyProvider.getProperty("bucketName");
		this.objectNamePrefix = propertyProvider.getProperty("objectNamePrefix");
		this.clickHouseUrl = propertyProvider.getProperty("clickHouseUrl");
		this.clickHouseTableName = propertyProvider.getProperty("clickHouseTableName");

	}

	/**
	 * to list all files in a gcs bucket
	 * 
	 * @return
	 */
	public List<String> fetchFiles() {
		List<String> filePaths = new ArrayList<>();
		Iterable<Blob> blobs = storage.get(bucketName).list(Storage.BlobListOption.prefix(objectNamePrefix))
				.iterateAll();
		for (Blob blob : blobs) {
			filePaths.add(blob.getName());
		}
		return filePaths;
	}

	/**
	 * method is to get content of a txt file from clod storage gcs
	 * 
	 * @return
	 */
	public String getContent() {
		Blob blob = storage.get(bucketName, "input.txt");
		return new String(blob.getContent());
	}

	/**
	 * this method is to upload data to clickhouse table
	 */
	public void processElement() {
		List<String[]> dataRecords = fetchDataFromGCSbucket();

		for (String[] data : dataRecords) {
			try {
				ClickHouseUploader uploader = new ClickHouseUploader(propertyProvider);
				uploader.uploadToClickHouse(Integer.parseInt(data[0]), data[1], Integer.parseInt(data[2]));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * this method is to fetch data from a csv file in gcs storage
	 * 
	 * @return
	 */
	public List<String[]> fetchDataFromGCSbucket() {
		List<String[]> dataRecords = new ArrayList<>();

		Blob blob = storage.get(BlobId.of(bucketName, objectNamePrefix.concat(".csv")));

		if (blob != null) {
			try (Reader reader = new StringReader(new String(blob.getContent()));
					CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withHeader())) {

				for (CSVRecord csvRecord : csvParser) {
					String[] recordData = new String[3];
					recordData[0] = csvRecord.get("id");
					recordData[1] = csvRecord.get("name");
					recordData[2] = csvRecord.get("age");
					dataRecords.add(recordData);
				}

			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {
			System.out.println("Blob not found: " + objectNamePrefix.concat(".csv"));
		}

		return dataRecords;
	}
}
