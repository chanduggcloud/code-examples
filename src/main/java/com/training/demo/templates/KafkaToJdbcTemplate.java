package com.training.demo.templates;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.List;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO.PreparedStatementSetter;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.codehaus.jettison.json.JSONException;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auth.oauth2.ServiceAccountCredentials;

public class KafkaToJdbcTemplate {

  private static final Logger logger = LoggerFactory.getLogger(KafkaToJdbcTemplate.class);

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

    pipeline
        .apply(KafkaIO.<Long, String>read().withBootstrapServers("localhost:9092").withTopic("beamTopic")
            .withKeyDeserializer(LongDeserializer.class).withValueDeserializer(StringDeserializer.class)
            .withoutMetadata())
        // .apply(Values.<String>create())
        .apply(Window.into(FixedWindows.of(Duration.standardSeconds(10))))

        .apply(ParDo.of(new DoFn<KV<Long, String>, String>() {

          private static final long serialVersionUID = 1L;

          @ProcessElement
          public void processElement(ProcessContext c) {
            System.out.println("Average age of customer: " + c.element());

            if (c.element().toString() != null) {
              // c.output(KV.of(c.element().getKey().toString(),
              // c.element().getValue().toString()));

              c.output(c.element().getValue().toString());
            }
          }
        }))
        .apply(Window.<String>into(FixedWindows.of(Duration.standardSeconds(30)))
            .triggering(AfterWatermark.pastEndOfWindow()).withAllowedLateness(Duration.ZERO).discardingFiredPanes())
        .apply(JdbcIO.<String>write()
            .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
                .create("com.mysql.jdbc.driver", "jdbc:mysql://127.0.0.1:3306/beamsdb/...").withUsername("root")
                .withPassword("root"))
            .withStatement("insert into event values(?,?)")
            .withPreparedStatementSetter(new PreparedStatementSetter<String>() {

              @Override
              public void setParameters(String element, PreparedStatement preparedStatement) throws Exception {
                logger.info("###### key = {}, value = {}", element.toString(), element.toString());
                preparedStatement.setString(1, element.toString());
                preparedStatement.setString(2, element.toString());

              }
            }));

    return pipeline.run();
  }

}
