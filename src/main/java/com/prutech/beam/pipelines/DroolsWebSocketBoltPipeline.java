package com.prutech.beam.pipelines;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.kafka.common.serialization.StringSerializer;

import com.prutech.beam.boltTransforms.DroolsWebSocketBolt;

public class DroolsWebSocketBoltPipeline {

	public static void buildPipeline() {

		PipelineOptions options = PipelineOptionsFactory.create();
		Pipeline pipeline = Pipeline.create(options);
		pipeline.apply("ReadFromTextFile", TextIO.read().from("src/main/resources/input.txt"))
				.apply("TransformToBeamEvents", ParDo.of(new DroolsWebSocketBolt()))
				.apply(KafkaIO.<String, String>write().withBootstrapServers("localhost:9092")
						.withTopic("driver_alerts_topic").withKeySerializer(StringSerializer.class)
						.withValueSerializer(StringSerializer.class));
		pipeline.run();
	}
}
