package com.training.demo.templates;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO.PreparedStatementSetter;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
/*import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.CopyWriter;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;*/
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.codehaus.jettison.json.JSONException;
import com.google.auth.oauth2.ServiceAccountCredentials;
import java.sql.PreparedStatement;
import com.training.demo.functions.TransformPubsubMessage;
import com.google.api.services.bigquery.model.TableRow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQueryToPubsub {

  private static final Logger logger = LoggerFactory.getLogger(BigQueryToPubsub.class);

  public interface Options extends DataflowPipelineOptions, StreamingOptions {

    @Description("BigQuery Table Query")
    @Required
    @Default.String("")
    ValueProvider<String> getBigQueryTable();

    void setBigQueryTable(ValueProvider<String> value);

    @Description("Service Account key file path")
    @Required
    @Default.String("/config/key.json")
    ValueProvider<String> getKeyFile();

    void setKeyFile(ValueProvider<String> value);

    @Description("The output topic")
    @Required
    ValueProvider<String> getOutputTopic();

    void setOutputTopic(ValueProvider<String> value);

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
        + "Ns (for seconds, example: 5s), " + "Nm (for minutes, example: 12m), " + "Nh (for hours, example: 2h).")
    @Default.String("5m")
    String getWindowDuration();

    void setWindowDuration(String value);
  }

  public static void main(String[] args) throws FileNotFoundException, IOException, JSONException {
    PipelineOptionsFactory.register(Options.class);
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    if (options.getKeyFile().isAccessible()) {
      final List<String> SCOPES = Arrays.asList("https://www.googleapis.com/auth/cloud-platform",
          "https://www.googleapis.com/auth/devstorage.full_control", "https://www.googleapis.com/auth/userinfo.email",
          "https://www.googleapis.com/auth/pubsub");
      options.setGcpCredential(
          ServiceAccountCredentials.fromStream(new FileInputStream(options.getKeyFile().get())).createScoped(SCOPES));
    }
    options.setStreaming(false);
    run(options);
  }

  public static PipelineResult run(Options options) throws JSONException, IOException {
    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);

    PCollection<TableRow> rows = getBQrows(pipeline, options);

    PCollection<PubsubMessage> pubsubMessages = transformToPubsubMessage(rows);

    publishToTopics(pubsubMessages);

    return pipeline.run();
  }

  public static PCollection<TableRow> getBQrows(Pipeline pipeline, Options options) {

    PCollection<TableRow> rows = pipeline.apply("Read from BigQuery",
        BigQueryIO.readTableRows().from(options.getBigQueryTable()));

    return rows;
  }

  public static PCollection<PubsubMessage> transformPubsubMessage(PCollection<TableRow> rows) {

    logger.info("#### transformToPubsubMessage: START");

    PCollection<PubsubMessage> pubsubMessages = rows.apply("Conver to PubsubMessage",
        ParDo.of(new TransformPubsubMessage()));

    logger.info("#### transformToPubsubMessage: END");

    return pubsubMessages;
  }

  public static PCollection<PubsubMessage> transformToPubsubMessage(PCollection<TableRow> rows) {

    logger.info("#### transformToPubsubMessage: START");

    PCollection<PubsubMessage> pubsubMessages = rows.apply(MapElements.into(new TypeDescriptor<PubsubMessage>() {
      private static final long serialVersionUID = 1L;
    }).via(e -> {
      assert e != null;
      System.out
          .println("### -> message : " + e.get("message") + " ; ---> replayTopic : " + e.get("replayTopic").toString());
      return new PubsubMessage(Objects.requireNonNull(e.get("message")).toString().getBytes(StandardCharsets.UTF_8),
          Collections.emptyMap()).withTopic(Objects.requireNonNull(e.get("replayTopic").toString()));
    }));

    logger.info("#### transformToPubsubMessage: END");

    return pubsubMessages;
  }

  public static void publishToJdbc(PCollection<KV<String, Long>> metrics) {

    metrics
        .apply(JdbcIO.<KV<String, Long>>write()
            .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
                .create("com.mysql.jdbc.driver", "jdbc:mysql://127.0.0.1:3306/beamsdb/...").withUsername("root")
                .withPassword("root"))
            .withStatement("insert into event values(?,?)")
            .withPreparedStatementSetter(new PreparedStatementSetter<KV<String, Long>>() {

              @Override
              public void setParameters(KV<String, Long> element, PreparedStatement preparedStatement)
                  throws Exception {
                preparedStatement.setString(1, element.getKey());
                preparedStatement.setLong(2, element.getValue());

              }
            }));
  }

  public static void publishToTopics(PCollection<PubsubMessage> pubsubMessages) {

    pubsubMessages.apply(PubsubIO.writeMessagesDynamic());
  }
}
