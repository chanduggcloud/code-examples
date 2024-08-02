package com.training.demo.windowing;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;

import java.io.IOException;
import java.io.StringReader;

public class Windowing_v6 {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<MovieTag> movieTags = pipeline
                .apply("ReadMovieTags",
                        TextIO.read().from("src/main/resources/source/data/windowing/tags_sample.csv"))
                .apply("ParseMovieTags",
                        ParDo.of(new ParseMovieTags()))
                .apply("Timestamps",
                        WithTimestamps.of(MovieTag::getTimestamp));

        movieTags.apply("Window", Window.into(new GlobalWindows()))
                .apply("ExtractTags", MapElements
                        .into(TypeDescriptors.strings())
                        .via(MovieTag::getTag))
                .apply(Count.perElement())
                .apply("ExtractTags", MapElements
                        .into(TypeDescriptors.strings())
                        .via(kv -> kv.getKey() + ", " + kv.getValue()))
                .apply("WriteToFile", TextIO
                        .write()
                        .to("src/main/resources/sink/windowing/v6/global_window_output").withSuffix(".csv")
                        .withHeader("Tag, Count")
                        .withNumShards(1)
                        .withWindowedWrites());

        pipeline.run().waitUntilFinish();
    }

    private static class ParseMovieTags extends DoFn<String, MovieTag> {

        @ProcessElement
        public void processElement(ProcessContext c) throws IOException {
            final CSVParser parser = new CSVParser(new StringReader(c.element()), CSVFormat.DEFAULT
                    .withDelimiter(',')
                    .withHeader(MovieTag.FILE_HEADER_MAPPING));

            CSVRecord record = parser.getRecords().get(0);

            // Skip over the header row
            if (record.get("timestamp").contains("timestamp") ){
                return;
            }

            DateTimeZone timeZone = DateTimeZone.forID("UTC");

            DateTime startedWatching = LocalDateTime.parse(record.get("timestamp").trim(),
                    DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")).toDateTime(timeZone);

            MovieTag movieTag = new MovieTag();
            movieTag.setUserId(record.get("userId"));
            movieTag.setSessionId(Integer.valueOf(record.get("sessionId")));
            movieTag.setMovieId(record.get("movieId").trim());
            movieTag.setTag(record.get("tag").trim());
            movieTag.setTimestamp(startedWatching.toInstant());

            c.output(movieTag);
        }
    }
}
