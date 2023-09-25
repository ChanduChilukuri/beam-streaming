package com.prutech.beam.pipelines;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;

import com.prutech.beam.boltTransforms.KafkaPublisher;
import com.prutech.beam.boltTransforms.WebSocketBolt;

public class WebSocketBoltPipeline {

	public static void buildPipeline() {

		PipelineOptions options = PipelineOptionsFactory.create();
		Pipeline pipeline = Pipeline.create(options);

		pipeline.apply("ReadFromTextFile", TextIO.read().from("src/main/resources/infraction.txt"))
				.apply("TransformToBeamEvents", ParDo.of(new WebSocketBolt()))
				.apply("kafka Producer", ParDo.of(new KafkaPublisher()));
		pipeline.run();
	}
}
