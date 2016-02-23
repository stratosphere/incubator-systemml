package org.apache.sysml.runtime.instructions.flink.utils;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.sysml.lops.PartialAggregate;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.functionobjects.KahanPlus;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.data.OperationsOnMatrixValues;
import org.apache.sysml.runtime.matrix.operators.AggregateOperator;
import org.apache.sysml.udf.Matrix;

public class DataSetAggregateUtils {

    public static DataSet<Tuple2<MatrixIndexes, MatrixBlock>> mergeByKey(DataSet<Tuple2<MatrixIndexes, MatrixBlock>> input) {
        return input.groupBy(0).reduce(new MergeBlocksFunction());
    }

    public static MatrixBlock sumStable(DataSet<MatrixBlock> input) throws DMLRuntimeException{

        try {
            return input.reduce(new SumSingleBlockFunction()).collect().get(0);
        } catch (Exception e) {
            throw new DMLRuntimeException("Could not collect final block of " + input);
        }
    }

    private static class SumSingleBlockFunction implements ReduceFunction<MatrixBlock> {

        private AggregateOperator _op = null;
        private MatrixBlock _corr = null;

        public SumSingleBlockFunction()
        {
            _op = new AggregateOperator(0, KahanPlus.getKahanPlusFnObject(), true, PartialAggregate.CorrectionLocationType.NONE);
            _corr = null;
        }

        @Override
        public MatrixBlock reduce(MatrixBlock value1, MatrixBlock value2) throws Exception {

            //create correction block (on demand)
            if( _corr == null ){
                _corr = new MatrixBlock(value1.getNumRows(), value1.getNumColumns(), false);
            }

            //copy one input to output
            MatrixBlock out = new MatrixBlock(value1);

            //aggregate other input
            OperationsOnMatrixValues.incrementalAggregation(out, _corr, value2, _op, false);

            return out;
        }
    }

    private static class MergeBlocksFunction implements ReduceFunction<Tuple2<MatrixIndexes, MatrixBlock>> {
        @Override
        public Tuple2<MatrixIndexes, MatrixBlock> reduce(Tuple2<MatrixIndexes, MatrixBlock> t1, Tuple2<MatrixIndexes, MatrixBlock> t2) throws Exception {
            final MatrixBlock b1 = t1.f1;
            final MatrixBlock b2 = t2.f1;

            // sanity check input dimensions
            if (b1.getNumRows() != b2.getNumRows() || b1.getNumColumns() != b2.getNumColumns()) {
                throw new DMLRuntimeException("Mismatched block sizes for: "
                        + b1.getNumRows() + " " + b1.getNumColumns() + " "
                        + b2.getNumRows() + " " + b2.getNumColumns());
            }

            // execute merge (never pass by reference)
            MatrixBlock ret = new MatrixBlock(b1);
            ret.merge(b2, false);
            ret.examSparsity();

            // sanity check output number of non-zeros
            if (ret.getNonZeros() != b1.getNonZeros() + b2.getNonZeros()) {
                throw new DMLRuntimeException("Number of non-zeros does not match: "
                        + ret.getNonZeros() + " != " + b1.getNonZeros() + " + " + b2.getNonZeros());
            }

            return new Tuple2<MatrixIndexes, MatrixBlock>(t1.f0, ret);
        }
    }
}
