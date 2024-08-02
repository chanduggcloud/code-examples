package com.training.demo.templates;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.codehaus.jettison.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.training.demo.functions.TransformToString;
import com.training.demo.util.BigQueryConverters;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.auth.oauth2.ServiceAccountCredentials;

import autovalue.shaded.com.google.common.collect.ImmutableList;

public class PubsubToBigQuery {

	private static final Logger logger = LoggerFactory.getLogger(BigQueryToPubsub.class);
	
	public interface Options extends DataflowPipelineOptions, StreamingOptions {

		@Description("BigQuery Table Query")
		@Required
		@Default.String("")
		ValueProvider<String> getPubsubSubscription();

		void setPubsubSubscription(ValueProvider<String> value);

		@Description("Service Account key file path")
		@Required
		@Default.String("/config/key.json")
		ValueProvider<String> getKeyFile();

		void setKeyFile(ValueProvider<String> value);

		@Description("The output topic")
		@Required
		ValueProvider<String> getOutputTable();

		void setOutputTable(ValueProvider<String> value);

		@Description("The directory to output temporary files to. Must end with a slash.")
		ValueProvider<String> getUserTempLocation();

		void setUserTempLocation(ValueProvider<String> value);

		@Description("The shard template of the output file. Specified as repeating sequences "
				+ "of the letters 'S' or 'N' (example: SSS-NNN). These are replaced with the "
				+ "shard number, or number of shards respectively")
		@Default.String("W-P-SS-of-NN")
		ValueProvider<String> getOutputShardTemplate();

		void setOutputShardTemplate(ValueProvider<String> value);

		@Description("The maximum number of output shards produced when writing.")
		@Default.Integer(1)
		Integer getNumShards();

		void setNumShards(Integer value);

		@Description("The window duration in which data will be written. Defaults to 5m. " + "Allowed formats are: "
				+ "Ns (for seconds, example: 5s), " + "Nm (for minutes, example: 12m), "
				+ "Nh (for hours, example: 2h).")
		@Default.String("5m")
		String getWindowDuration();

		void setWindowDuration(String value);
	}

	public static void main(String[] args) throws FileNotFoundException, IOException, JSONException {
		PipelineOptionsFactory.register(Options.class);
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

		if (options.getKeyFile().isAccessible()) {
			final List<String> SCOPES = Arrays.asList("https://www.googleapis.com/auth/cloud-platform",
					"https://www.googleapis.com/auth/devstorage.full_control",
					"https://www.googleapis.com/auth/userinfo.email", "https://www.googleapis.com/auth/pubsub");
			options.setGcpCredential(ServiceAccountCredentials
					.fromStream(new FileInputStream(options.getKeyFile().get())).createScoped(SCOPES));
		}
		options.setStreaming(false);
		
		PipelineResult pipelineResult = run(options);
		
		
		queryAndPrintMetricsResults(pipelineResult, "InputRecords", "counter");
    queryAndPrintMetricsResults(pipelineResult, "CarPrices", "distribution");
    queryAndPrintMetricsResults(pipelineResult, "PriceThreshold", "gauge");
	}

	public static PipelineResult run(Options options) throws JSONException, IOException {
		// Create the pipeline
		Pipeline pipeline = Pipeline.create(options);
		
		final TupleTag<String> success = new TupleTag<String>(){
			private static final long serialVersionUID = 1L;
		};
		final TupleTag<String> failure = new TupleTag<String>(){

			private static final long serialVersionUID = 1L;
		};

		PCollection<PubsubMessage> messages = getPubSubMessages(pipeline, options);
		
		PCollectionTuple collectionTuple = transformToString(messages, success, failure);

		//collectionTuple.get(failure).apply("Step1-JsonToPojoTransformFn DLQ topic", PubsubIO.writeStrings().to(options.getMappdlqTopic()));
		
		PCollection<TableRow> bigQueryRecords = transformToTableRow(collectionTuple, success);
		
		writeToBigQuery(bigQueryRecords, options);

		return pipeline.run();
	}
	public static PCollection<PubsubMessage> getPubSubMessages(Pipeline pipeline, Options options) {
		
		PCollection<PubsubMessage> messages = pipeline.apply("Read messages from pubsub",
				PubsubIO.readMessages().fromSubscription(options.getPubsubSubscription()));

		
		return messages;
	}
	
	public static PCollectionTuple transformToString(PCollection<PubsubMessage> messages, TupleTag<String> success, TupleTag<String> failure) {
		
		logger.info("#### transformToPubsubMessage: START");
		
		PCollectionTuple collectionTuple = messages.apply("Conver to String", ParDo.of(new TransformToString(success, failure))
															.withOutputTags(success, TupleTagList.of(failure)));
		
		logger.info("#### transformToPubsubMessage: END");
		
		return collectionTuple;
	}
	
	public static PCollection<TableRow> transformToTableRow(PCollectionTuple collectionTuple, TupleTag<String> success) {
		
		logger.info("#### transformToPubsubMessage: START");
		
		// Convert JSON to TableRow
        PCollection<TableRow> bigQueryRecords = collectionTuple.get(success)
                .apply("Convert JSON to TableRow", BigQueryConverters.jsonToTableRow())
                .setCoder(TableRowJsonCoder.of());
        
		logger.info("#### transformToPubsubMessage: END");
		
		return bigQueryRecords;
	}
	
	public static void writeToBigQuery(PCollection<TableRow> bigQueryRecords, Options options) {
		
		bigQueryRecords.apply("Insert into Bigquery",
				BigQueryIO.writeTableRows().withSchema(tableSchema).to(options.getOutputTable())
						.withTimePartitioning(new TimePartitioning().setField("logDate").setType("DAY"))
						.withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
						.withWriteDisposition(WriteDisposition.WRITE_APPEND));
		
		
	}
	private static void queryAndPrintMetricsResults(PipelineResult result, String namespace, String name) {
	     MetricQueryResults metrics = result.metrics().queryMetrics(MetricsFilter.builder().addNameFilter(
	     MetricNameFilter.named(namespace,name)).build()); 
	     for(MetricResult<Long> counter: metrics.getCounters()) { 
	       logger.info("####### {}: {}/{}", counter.getName(), counter.getCommitted(), counter.getAttempted()); 
	     }
	     for(MetricResult<DistributionResult> distribution: metrics.getDistributions()) { 
	       logger.info("####### {}: {} mean: {}", distribution.getName(),
	       distribution.getCommitted(), distribution.getCommitted().getMean()); 
	     }
	    
	}
	static TableSchema tableSchema = new TableSchema().setFields(
			ImmutableList.of(new TableFieldSchema().setName("dlqTopicName").setType("STRING").setMode("NULLABLE"),
					new TableFieldSchema().setName("raw").setType("STRING").setMode("NULLABLE"),
					new TableFieldSchema().setName("logDate").setType("TIMESTAMP").setMode("NULLABLE"),
					new TableFieldSchema().setName("status").setType("STRING").setMode("NULLABLE"),
					new TableFieldSchema().setName("replayTopic").setType("STRING").setMode("NULLABLE"),
					new TableFieldSchema().setName("ERROR_MESSAGE").setType("STRING").setMode("NULLABLE"),
					new TableFieldSchema().setName("trace_id").setType("STRING").setMode("NULLABLE"),
					new TableFieldSchema().setName("message").setType("STRING").setMode("NULLABLE"),
					new TableFieldSchema().setName("updated_message").setType("STRING").setMode("NULLABLE")));
}
