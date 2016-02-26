package org.apache.sysml.runtime.instructions.flink.utils;

import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class IndexUtils {

    private static <T> DataSet<Tuple2<Integer, Long>> countElements(DataSet<Tuple2<Integer, T>> input) {
        return input.mapPartition(new RichMapPartitionFunction<Tuple2<Integer, T>, Tuple2<Integer, Long>>() {
            @Override
            public void mapPartition(Iterable<Tuple2<Integer, T>> iterable, Collector<Tuple2<Integer, Long>> collector) throws Exception {
                Iterator<Tuple2<Integer, T>> itr = iterable.iterator();
                if (itr.hasNext()) {
                    int splitIndex = itr.next().f0;
                    long counter = 1L;
                    for (; itr.hasNext(); ++counter) {
                        Tuple2<Integer, T> value = itr.next();
                        if (value.f0 != splitIndex) {
                            collector.collect(new Tuple2<Integer, Long>(splitIndex, counter));
                            splitIndex = value.f0;
                            counter = 0L;
                        }
                    }
                    collector.collect(new Tuple2<Integer, Long>(splitIndex, counter));
                }
            }
        });
    }

    public static <T> DataSet<Tuple2<Long, T>> zipWithRowIndex(DataSet<Tuple2<Integer, T>> input) {
        DataSet<Tuple2<Integer, Long>> elementCount = countElements(input);
        return input.mapPartition(new RichMapPartitionFunction<Tuple2<Integer, T>, Tuple2<Long, T>>() {
            private long[] splitOffsets;
            private long[] splitCounts;

            @Override
            public void open(Configuration parameters) throws Exception {
                List<Tuple2<Integer, Long>> tmp = this.getRuntimeContext().<Tuple2<Integer, Long>>getBroadcastVariable("counts");
                tmp.sort(new Comparator<Tuple2<Integer, Long>>() {
                    @Override
                    public int compare(Tuple2<Integer, Long> o1, Tuple2<Integer, Long> o2) {
                        return o1.f0 - o2.f0;
                    }
                });
                this.splitOffsets = new long[tmp.size()];
                this.splitCounts = new long[tmp.size()];
                for (int i = 1; i < tmp.size(); i++) {
                    splitOffsets[i] = tmp.get(i - 1).f1 + splitOffsets[i - 1];
                }
            }

            @Override
            public void mapPartition(Iterable<Tuple2<Integer, T>> values, Collector<Tuple2<Long, T>> out) throws Exception {
                for (Tuple2<Integer, T> value : values) {
                    out.collect(new Tuple2<Long, T>(this.splitOffsets[value.f0] + this.splitCounts[value.f0]++, value.f1));
                }
            }
        }).withBroadcastSet(elementCount, "counts");

    }
}
