package com.training.demo.combine;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import javax.annotation.Nullable;
import java.io.Serializable;

public class Combining_v2 {

    private static final String CSV_INFO_HEADER = "CustomerID,Gender,Age,Annual_Income";

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<KV<String, Double>> genderAges = pipeline
                .apply(TextIO.read().from("src/main/resources/source/mall_customers_info.csv"))
                .apply("FilterInfoHeader", ParDo.of(new FilterHeaderFn(CSV_INFO_HEADER)))
                .apply("GenderAgeKV", ParDo.of(new GenderAgeKVFn()));

        genderAges.apply("CombinePerKeyAggregation", Combine.perKey(new AverageFn()))
            .apply("PrintToConsole", ParDo.of(new DoFn<KV<String, Double>, Void>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        System.out.println("Average age: " +
                                c.element().getKey() + ", " + c.element().getValue());
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

    private static class GenderAgeKVFn extends DoFn<String, KV<String, Double>> {

        @ProcessElement
        public void processElement(
                @Element String element,
                OutputReceiver<KV<String, Double>> out) {
            String[] fields = element.split(",");

            String gender = fields[1];
            double age = Double.parseDouble(fields[2]);

            out.output(KV.of(gender, age));
        }
    }

    private static class AverageFn extends Combine.CombineFn<Double, AverageFn.AverageAccumulator, Double> {

        public static class AverageAccumulator implements Serializable {

            private static final long serialVersionUID = 42L;

            double sum = 0;
            int count = 0;

            @Override
            public boolean equals(@Nullable Object o) {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }

                AverageAccumulator accum = (AverageAccumulator) o;
                return sum == accum.sum &&
                        count == accum.count;
            }

        }

        @Override
        public AverageAccumulator createAccumulator() { return new AverageAccumulator(); }

        @Override
        public AverageAccumulator addInput(AverageAccumulator accum, Double input) {
            accum.sum += input;
            accum.count++;

            return accum;
        }

        @Override
        public AverageAccumulator mergeAccumulators(Iterable<AverageAccumulator> accumulators) {
            AverageAccumulator merged = createAccumulator();

            for (AverageAccumulator accum : accumulators) {
                merged.sum += accum.sum;
                merged.count += accum.count;
            }

            return merged;
        }

        @Override
        public Double extractOutput(AverageAccumulator accumulator) {
            return accumulator.sum / accumulator.count;
        }

    }
}
