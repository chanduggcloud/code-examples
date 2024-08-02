package com.training.demo.combine;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;

public class Combining_v1 {

    private static final String CSV_INFO_HEADER = "CustomerID,Gender,Age,Annual_Income";

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<Double> ages = pipeline.apply(TextIO.read().from("src/main/resources/source/mall_customers_info.csv"))
                .apply("FilterInfoHeader", ParDo.of(new FilterHeaderFn(CSV_INFO_HEADER)))
                .apply("ExtractAge", ParDo.of(new ExtractAgeFn()));

        ages.apply("CombineAggregation", Combine.globally(new AverageFn()))
            .apply("PrintToConsole", ParDo.of(new DoFn<Double, Void>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        System.out.println("Average age of customer: " + c.element());
                    }
            }));;

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

    private static class ExtractAgeFn extends DoFn<String, Double> {

        @ProcessElement
        public void processElement(
                @Element String element,
                OutputReceiver<Double> out) {
            String[] fields = element.split(",");

            double age = Double.parseDouble(fields[2]);

            out.output(age);
        }
    }

    private static class AverageFn implements SerializableFunction<Iterable<Double>, Double> {

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

}
