package com.training.demo.partition;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

public class Partitioning {

    private static final String CSV_HEADER =
            "car,price,body,mileage,engV,engType,registration,year,model,drive";

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<KV<String, Double>> makePriceKV = pipeline
                .apply("ReadAds", TextIO.read().from("src/main/resources/source/car_ads*.csv"))
                .apply("FilterHeader", ParDo.of(new FilterHeaderFn(CSV_HEADER)))
                .apply("MakePriceKVFn", ParDo.of(new MakePriceKVFn()));

        PCollectionList<KV<String, Double>> priceCategories = makePriceKV
                .apply(Partition.of(4, new Partition.PartitionFn<KV<String, Double>>() {
                    @Override
                    public int partitionFor(KV<String, Double> elem, int numPartitions) {
                        if (elem.getValue() < 2000) {
                            return 0;
                        } else if (elem.getValue() < 5000) {
                            return 1;
                        } else if (elem.getValue() < 10000) {
                            return 2;
                        }

                        return 3;
                    }
                }));

        priceCategories.get(2)
                .apply("PrintToConsole", ParDo.of(new DoFn<KV<String, Double>, Void>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        System.out.println(c.element().getKey() + ": " + c.element().getValue());
                    }
                }));

        pipeline.run().waitUntilFinish();
    }

    private static class FilterHeaderFn extends DoFn<String, String> {

        private final String header;

        public FilterHeaderFn(String header) {
            this.header = header;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            String row = c.element();

            if (!row.isEmpty() && !row.equals(this.header)) {
                c.output(row);
            }
        }
    }

    private static class MakePriceKVFn extends DoFn<String, KV<String, Double>> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] fields = c.element().split(",");

            String make = fields[0];
            Double price = Double.parseDouble(fields[1]);

            c.output(KV.of(make, price));
        }
    }
}
