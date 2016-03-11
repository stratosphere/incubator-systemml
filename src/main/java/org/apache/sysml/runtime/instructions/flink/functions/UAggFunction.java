package org.apache.sysml.runtime.instructions.flink.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.data.OperationsOnMatrixValues;
import org.apache.sysml.runtime.matrix.operators.AggregateUnaryOperator;

public class UAggFunction implements MapFunction<Tuple2<MatrixIndexes, MatrixBlock>,
                                                 Tuple2<MatrixIndexes, MatrixBlock>> {
    private static final long serialVersionUID = 2672082409287856038L;

    private AggregateUnaryOperator _op = null;
    private int _brlen = -1;
    private int _bclen = -1;

    public UAggFunction(AggregateUnaryOperator op, int brlen, int bclen)
    {
        _op = op;
        _brlen = brlen;
        _bclen = bclen;
    }

    @Override
    public Tuple2<MatrixIndexes, MatrixBlock> map(Tuple2<MatrixIndexes, MatrixBlock> value) throws Exception {
        MatrixIndexes ixIn = value.f0;
        MatrixBlock blkIn = value.f1;

        MatrixIndexes ixOut = new MatrixIndexes();
        MatrixBlock blkOut = new MatrixBlock();

        //unary aggregate operation (always keep the correction)
        OperationsOnMatrixValues.performAggregateUnary(ixIn, blkIn,
                ixOut, blkOut, _op, _brlen, _bclen);

        value.f0 = ixOut;
        value.f1 = blkOut;
        return value;
    }
}
