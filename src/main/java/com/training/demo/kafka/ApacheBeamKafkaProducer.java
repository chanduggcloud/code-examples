package com.training.demo.kafka;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
//import org.apache.

public class ApacheBeamKafkaProducer {

	public static void main(String[] args) {
		
		PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        
        PCollection pcollection = pipeline.apply(Create.of("Hello!"));
        
        /*pcollection.apply(KafkaIO.<Void, String>write()
        		.withBootstrapServers("localhost:9092")
        		.withTopic("test")
        		.withValueSerializer(StringSerializer.class).values());*/
        		
        pipeline.run().waitUntilFinish();

	}

}
