package org.apache.sysml.runtime.instructions.flink.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.operators.ScalarOperator;

/**
 * Flink map function that applies a scalar operation to a blocks.
 */
public class MatrixScalarFunction implements MapFunction<Tuple2<MatrixIndexes, MatrixBlock>, Tuple2<MatrixIndexes, MatrixBlock>> {

    private final ScalarOperator operator;
    private final Tuple2<MatrixIndexes, MatrixBlock> output = new Tuple2<MatrixIndexes, MatrixBlock>();

    public MatrixScalarFunction(ScalarOperator operator) {
        this.operator = operator;
    }

    @Override
    public Tuple2<MatrixIndexes, MatrixBlock> map(Tuple2<MatrixIndexes, MatrixBlock> value) throws Exception {
        output.f0 = value.f0;
        output.f1 = (MatrixBlock) value.f1.scalarOperations(operator, new MatrixBlock());
        return output;
    }
}
