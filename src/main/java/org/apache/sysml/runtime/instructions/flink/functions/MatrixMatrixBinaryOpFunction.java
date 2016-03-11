package org.apache.sysml.runtime.instructions.flink.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.operators.BinaryOperator;

public class MatrixMatrixBinaryOpFunction implements JoinFunction<Tuple2<MatrixIndexes, MatrixBlock>,
                                                                  Tuple2<MatrixIndexes, MatrixBlock>,
                                                                  Tuple2<MatrixIndexes, MatrixBlock>> {

    private static final long serialVersionUID = -2683276102742977900L;
    private final Tuple2<MatrixIndexes, MatrixBlock> output = new Tuple2<MatrixIndexes, MatrixBlock>();
    private BinaryOperator _bop;

    public MatrixMatrixBinaryOpFunction(BinaryOperator op) {
        _bop = op;
    }

    @Override
    public Tuple2<MatrixIndexes, MatrixBlock> join(Tuple2<MatrixIndexes, MatrixBlock> first, Tuple2<MatrixIndexes, MatrixBlock> second) throws Exception {
        output.f0 = first.f0;
        output.f1 = (MatrixBlock) first.f1.binaryOperations(_bop, second.f1, new MatrixBlock());;
        return output;
    }
}
