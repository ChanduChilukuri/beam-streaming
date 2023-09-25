
package com.prutech.beam.utils;

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface MyCustomPipelineOption extends PipelineOptions, GcpOptions {

	// service josn key of service member
	@Description("GCS JSON Key File Path")
	@Default.String("C:/Users/chandu.ch/Downloads/truck-demo-396806-0d851ef48b0c.json")
	String getGcsJsonKeyFilePath();

	void setGcsJsonKeyFilePath(String jsonKeyFilePath);

	// project id
	@Description("GCS Project ID")
	@Default.String("truck-demo-396806")
	String getGcsProjectId();

	void setGcsProjectId(String projectId);

	// get input to pcollection
	@Description("Input for the pipeline")
	@Default.String("gs://truck-bucket-demo/input.txt")
	String getInput();

	void setInput(String input);

	// pcollection output
	@Description("Output for the pipeline")
	@Default.String("gs://truck-bucket-demo/output.txt")
	String getOutput();

	void setOutput(String output);

}
