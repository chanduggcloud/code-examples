package com.training.demo.sideInput;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;

import java.util.Collections;

public class SideInput {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> googStockPrices = pipeline.apply("ReadGoogStockPrices",
                TextIO.read().from("src/main/resources/source/googStockPrices2020.csv"));

        final PCollectionView<Double> globalAveragePrice = googStockPrices
                .apply("ExtractCloseValues", FlatMapElements
                        .into(TypeDescriptors.doubles())
                        .via(csvRow -> Collections.singletonList(
                                Double.parseDouble(csvRow.split(",")[5]))))
                .apply("GlobalAverage",
                        Combine.globally(new Average()).asSingletonView());

        PCollection<KV<Integer, Double>> averagePerMonth = googStockPrices
                .apply("ExtractMonthPrices", ParDo.of(new MakeMonthPriceKVFn()))
                .apply(Combine.perKey(new Average()));

        averagePerMonth.apply(ParDo.of(new DoFn<KV<Integer, Double>, Void>() {

            @ProcessElement
            public void processElement(ProcessContext c)  {
                double globalAverage = c.sideInput(globalAveragePrice);

                if (c.element().getValue() >= globalAverage) {
                    System.out.println("Month " + c.element().getKey() + ": " + c.element().getValue());
                }
            }
        }).withSideInputs(globalAveragePrice));

        pipeline.run().waitUntilFinish();
    }

    private static class Average implements SerializableFunction<Iterable<Double>, Double> {

        @Override
        public Double apply(Iterable<Double> input) {
            double sum = 0;
            int count = 0;
            for (double item : input) {
                sum += item;
                count = count + 1;
            }

            return sum / count;
        }

    }

    private static class MakeMonthPriceKVFn extends DoFn<String, KV<Integer, Double>> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] fields = c.element().split(",");

            DateTimeZone timeZone = DateTimeZone.forID("UTC");

            DateTime dateTime = LocalDateTime.parse(fields[0].trim(),
                    DateTimeFormat.forPattern("yyyy-MM-dd")).toDateTime(timeZone);

            c.output(KV.of(dateTime.getMonthOfYear(), Double.parseDouble(fields[5])));
        }
    }

}
