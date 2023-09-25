package com.prutech.beam.clickhouse;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import com.google.cloud.storage.Blob;
import com.prutech.beam.utils.PropertyProvider;

/**
 * create partitions then store data in partitions
 * 
 * @author chandu.ch
 *
 */
public class ExtractAndUploadData extends DoFn<String, Void> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -426525614485236561L;

	private final String clickHouseUrl;
	private final String clickHouseTableName;
	private final PropertyProvider propertyProvider;

	public ExtractAndUploadData(PropertyProvider propertyProvider) {

		this.clickHouseUrl = propertyProvider.getProperty("clickHouseUrl");
		this.clickHouseTableName = propertyProvider.getProperty("clickHouseTableName");
		this.propertyProvider = propertyProvider;
	}

	@ProcessElement
	public void processElement(ProcessContext c) {
		String filePath = c.element();
		String[] data = fetchDataFromFile(filePath);

		try {
			ClickHouseUploader clickHouseUploader = new ClickHouseUploader(propertyProvider);
			clickHouseUploader.uploadToClickHouse(Integer.parseInt(data[0]), data[1], Integer.parseInt(data[2]));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private String[] fetchDataFromFile(String filePath) {
		String[] data = new String[3]; // Assuming three columns: id, name, and age

		try (Reader reader = new FileReader(filePath); CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT)) {
			for (CSVRecord csvRecord : csvParser) {
				data[0] = csvRecord.get("id");
				data[1] = csvRecord.get("name");
				data[2] = csvRecord.get("age");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return data;
	}

	/**
	 * 
	 * @return
	 */
	public List<String[]> fetchDataFromGCSbucket(Blob blob) {
		List<String[]> dataRecords = new ArrayList<>();
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
			System.out.println("Blob not found: " + "");
		}

		return dataRecords;
	}

}