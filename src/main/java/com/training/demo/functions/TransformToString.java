package com.training.demo.functions;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Gauge;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.json.JSONObject;

public class TransformToString extends DoFn<PubsubMessage, String> implements Serializable {


	private static final long serialVersionUID = 1L;
	
	private final Counter inputRecordCounter = Metrics.counter("InputRecords", "counter");
	private final Distribution carPriceDistribution = Metrics.distribution("CarPrices", "distribution");
	private final Gauge thresholdPriceGauge = Metrics.gauge("PriceThreshold", "gauge");
	
	final TupleTag<String> success;
	final TupleTag<String> failure;
	
	public TransformToString(TupleTag<String> success, TupleTag<String> failure) {
		this.success = success;
		this.failure = failure;
	}

	@ProcessElement
    public void processElement(ProcessContext c) {
		PubsubMessage message = c.element();
		
		String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
		
		System.out.println("### -> message : " + message.getPayload().toString());
		
		try {
			JSONObject jsonMessage = new JSONObject(payload);
			inputRecordCounter.inc();
			carPriceDistribution.update(100);
	    thresholdPriceGauge.set(Math.round(100)); 
			
			c.output(success, jsonMessage.toString());
			
		} catch(Exception e) {
			c.output(failure, payload);
		}
    }
}