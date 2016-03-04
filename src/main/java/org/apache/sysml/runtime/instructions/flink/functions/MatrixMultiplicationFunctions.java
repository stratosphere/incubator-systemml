package org.apache.sysml.runtime.instructions.flink.functions;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.sysml.runtime.functionobjects.Multiply;
import org.apache.sysml.runtime.functionobjects.Plus;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.operators.AggregateBinaryOperator;
import org.apache.sysml.runtime.matrix.operators.AggregateOperator;
import org.apache.sysml.runtime.matrix.operators.BinaryOperator;

/**
 * Utility Flink functions for matrix multiplication.
 */
public final class MatrixMultiplicationFunctions {

    private MatrixMultiplicationFunctions() {}

    /**
     * Join function that multiplies two matrix blocks <code>A[m,n]</code> and <code>B[s,t]</code>, assuming
     * <code>n == s</code> and emits a new MatrixBlock <code>C[m,s] = A[m,n] * A[s,t]</code>.
     */
    public static class MultiplyMatrixBlocks implements JoinFunction<
            Tuple2<MatrixIndexes, MatrixBlock>, Tuple2<MatrixIndexes, MatrixBlock>,
            Tuple2<MatrixIndexes, MatrixBlock>> {

        private final Tuple2<MatrixIndexes, MatrixBlock> output = new Tuple2<MatrixIndexes, MatrixBlock>();

        //created operator for reuse
        private final AggregateOperator agg = new AggregateOperator(0, Plus.getPlusFnObject());
        private final AggregateBinaryOperator operation = new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), agg);

        @Override
        public Tuple2<MatrixIndexes, MatrixBlock> join(Tuple2<MatrixIndexes, MatrixBlock> first,
                                                       Tuple2<MatrixIndexes, MatrixBlock> second) throws Exception {
            assert second.f0.getRowIndex() == first.f0.getColumnIndex() : "Indices do not match";
            output.f0 = new MatrixIndexes(first.f0.getRowIndex(), second.f0.getColumnIndex());
            output.f1 = new MatrixBlock();
            first.f1.aggregateBinaryOperations(first.f1, second.f1, output.f1, operation);
            return output;
        }
    }

    /**
     * Reduce function that sums two matrix blocks <code>C[i,j]</code> and <code>C[m,n]</code>, assuming they have the
     * same index <code>i == m && j == n</code> and the block have the same dimensions
     * <code>nrow(A) == nrow(B) && ncol(A) == ncol(B)</code>.
     */
    @RichGroupReduceFunction.Combinable
    public static class SumMatrixBlocks implements ReduceFunction<Tuple2<MatrixIndexes, MatrixBlock>> {
        private final Tuple2<MatrixIndexes, MatrixBlock> output = new Tuple2<MatrixIndexes, MatrixBlock>();
        private final BinaryOperator plus = new BinaryOperator(Plus.getPlusFnObject());

        @Override
        public Tuple2<MatrixIndexes, MatrixBlock> reduce(Tuple2<MatrixIndexes, MatrixBlock> left,
                                                         Tuple2<MatrixIndexes, MatrixBlock> right) throws Exception {
            assert left.f0 == right.f0 : "Indices do not match";
            output.f0 = left.f0;
            output.f1 = new MatrixBlock();
            left.f1.binaryOperations(plus, right.f1, output.f1);
            return output;
        }
    }

    public static class SumMatrixBlocksCombine
            implements GroupCombineFunction<Tuple2<MatrixIndexes, MatrixBlock>, Tuple2<MatrixIndexes, MatrixBlock>> {

        private final Tuple2<MatrixIndexes, MatrixBlock> output = new Tuple2<MatrixIndexes, MatrixBlock>();
        private final BinaryOperator plus = new BinaryOperator(Plus.getPlusFnObject());

        @Override
        public void combine(Iterable<Tuple2<MatrixIndexes, MatrixBlock>> values,
                            Collector<Tuple2<MatrixIndexes, MatrixBlock>> out) throws Exception {
            output.f0 = null;
            output.f1 = null;
            for (Tuple2<MatrixIndexes, MatrixBlock> val : values) {
                if (output.f0 == null) {
                    output.f0 = val.f0;
                    output.f1 = val.f1;
                } else {
                    output.f1 = (MatrixBlock) output.f1.binaryOperations(plus, val.f1, new MatrixBlock());
                }
            }
            out.collect(output);
        }
    }

    /**
     * {@link KeySelector} implementation that retrieves the row index of a matrix block.
     */
    public static class RowSelector implements KeySelector<Tuple2<MatrixIndexes, MatrixBlock>, Long> {

        @Override
        public Long getKey(Tuple2<MatrixIndexes, MatrixBlock> value) throws Exception {
            return value.f0.getRowIndex();
        }
    }

    /**
     * {@link KeySelector} implementation that retrieves the column index of a matrix block.
     */
    public static class ColumnSelector implements KeySelector<Tuple2<MatrixIndexes, MatrixBlock>, Long> {

        @Override
        public Long getKey(Tuple2<MatrixIndexes, MatrixBlock> value) throws Exception {
            return value.f0.getColumnIndex();
        }
    }

    /**
     * {@link KeySelector} implementation that retrieves indexes of a matrix block.
     */
    public static class MatrixIndexesSelector implements KeySelector<Tuple2<MatrixIndexes, MatrixBlock>, MatrixIndexes> {

        @Override
        public MatrixIndexes getKey(Tuple2<MatrixIndexes, MatrixBlock> value) throws Exception {
            return value.f0;
        }
    }
}
