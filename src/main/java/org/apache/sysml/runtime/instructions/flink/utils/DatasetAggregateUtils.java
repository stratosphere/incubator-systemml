package org.apache.sysml.runtime.instructions.flink.utils;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;

import java.util.Iterator;

public class DatasetAggregateUtils {

    public static DataSet<Tuple2<MatrixIndexes, MatrixBlock>> mergeByKey(DataSet<Tuple2<MatrixIndexes, MatrixBlock>> input) {
        return input.groupBy(0).reduceGroup(new MergeBlocksFunction());
    }

    /**
     *
     */
    private static class MergeBlocksFunction implements GroupReduceFunction<Tuple2<MatrixIndexes, MatrixBlock>, Tuple2<MatrixIndexes, MatrixBlock>>
    {
        private static final long serialVersionUID = -8881019027250258850L;

        @Override
        public void reduce(Iterable<Tuple2<MatrixIndexes, MatrixBlock>> values, Collector<Tuple2<MatrixIndexes, MatrixBlock>> out) throws Exception {

            Iterator<Tuple2<MatrixIndexes, MatrixBlock>> iter = values.iterator();
            MatrixBlock ret = new MatrixBlock();
            Tuple2<MatrixIndexes, MatrixBlock> t = null;

            while (iter.hasNext()) {
                t = iter.next();
                // execute merge (never pass by reference)
                MatrixBlock b = new MatrixBlock(t.f1);
                ret.merge(b, false);

                 // TODO sanity and dimension checks
            }

            ret.examSparsity();
            out.collect(new Tuple2<MatrixIndexes, MatrixBlock>(t.f0, ret));
        }
    }
}
