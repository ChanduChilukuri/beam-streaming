package com.prutech.beam;

import java.io.IOException;
import java.sql.SQLException;

import com.google.cloud.storage.Storage;
import com.prutech.beam.utils.FileFetcher;
import com.prutech.beam.utils.GCSAuthenticator;
import com.prutech.beam.utils.PropertyLoader;
import com.prutech.beam.utils.PropertyProvider;

/**
 * 
 * @author chandu.ch
 *
 */
public class App {

	private final PropertyProvider propertyProvider;

	public App(PropertyProvider propertyProvider) {
		this.propertyProvider = propertyProvider;
	}

	public static void main(String[] args) throws IOException, SQLException {

		PropertyProvider propertyProvider = new PropertyLoader();
		// App app = new App(propertyProvider);
		// Authenticate with GCS
		GCSAuthenticator gcsAuthentication = new GCSAuthenticator(propertyProvider);
		Storage storage = gcsAuthentication.authenticate();

		FileFetcher fileFetcher = new FileFetcher(propertyProvider, storage);

		System.out.println(fileFetcher.getContent());
		// fileFetcher.processElement();
		System.out.println("-------------------------");

		// print csv file data
		/*
		 * List<String[]> fetchedData = fileFetcher.fetchDataFromGCSbucket(); for
		 * (String[] record : fetchedData) { System.out.println("ID: " + record[0] +
		 * ", Name: " + record[1] + ", Age: " + record[2]); }
		 */
		// creating table in clickhouse database

		/*
		 * ClickHouseUploader clickhouseUploader = new
		 * ClickHouseUploader(propertyProvider); try {
		 * clickhouseUploader.createTableWithPartitions(); } catch (SQLException e) {
		 * 
		 * e.printStackTrace(); }
		 */

		// beam pipelines
		/*
		 * PipelineOptionsFactory.register(MyCustomPipelineOption.class);
		 * MyCustomPipelineOption options =
		 * PipelineOptionsFactory.fromArgs(args).withValidation()
		 * .as(MyCustomPipelineOption.class); Pipeline pipeline =
		 * Pipeline.create(options);
		 * 
		 * pipeline.apply("ReadLines", TextIO.read().from(options.getInput()))
		 * .apply("ProcessLines",
		 * MapElements.into(TypeDescriptor.of(String.class)).via((String line) ->
		 * line.toUpperCase())) .apply("PrintResults",
		 * MapElements.into(TypeDescriptor.of(Void.class)).via((String line) -> {
		 * System.out.println(line); return null; }));
		 * 
		 * pipeline.run().waitUntilFinish();
		 */

		/*
		 * pipeline.apply("ReadFromStorage", TextIO.read().from("gs://" + bucketName +
		 * "/" + objectNamePrefix + ".csv")) .apply(ParDo.of(new
		 * ExtractAndUploadData(clickHouseUrl, clickHouseTableName)));
		 */
		/*
		 * BlobId blobId = BlobId.of(propertyProvider.getProperty("bucketName"),
		 * propertyProvider.getProperty("objectNamePrefix").concat(".csv")); Blob blob =
		 * storage.get(blobId);
		 * 
		 * // Upload data to ClickHouse if (blob != null) { InputStream inputStream =
		 * new ByteArrayInputStream(blob.getContent());
		 * 
		 * clickhouseUploader.uploadCsvFile("csv_table", inputStream);
		 * System.out.println("Data uploaded from GCS to ClickHouse."); } else {
		 * System.out.println("Blob not found in GCS."); } }
		 */

	}

}
