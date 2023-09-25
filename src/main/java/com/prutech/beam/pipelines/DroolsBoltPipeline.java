package com.prutech.beam.pipelines;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;

import com.prutech.beam.boltTransforms.DroolsBolt;

public class DroolsBoltPipeline {

	public static void buildPipeline() {

		PipelineOptions options = PipelineOptionsFactory.create();
		Pipeline pipeline = Pipeline.create(options);
		pipeline.apply(TextIO.read().from("src/main/resources/input.txt")).apply(ParDo.of(new DroolsBolt()))
				.apply(TextIO.write().to("output.txt").withoutSharding());
		pipeline.run();

	}

}
