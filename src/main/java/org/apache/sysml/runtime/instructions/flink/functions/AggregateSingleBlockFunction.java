package org.apache.sysml.runtime.instructions.flink.functions;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.OperationsOnMatrixValues;
import org.apache.sysml.runtime.matrix.operators.AggregateOperator;

public class AggregateSingleBlockFunction implements ReduceFunction<MatrixBlock> {

    private static final long serialVersionUID = -3672377410407066396L;

    private AggregateOperator _op = null;
    private MatrixBlock _corr = null;

    public AggregateSingleBlockFunction(AggregateOperator op)
    {
        _op = op;
        _corr = null;
    }

    @Override
    public MatrixBlock reduce(MatrixBlock value1, MatrixBlock value2) throws Exception {
        //copy one first input
        MatrixBlock out = new MatrixBlock(value1);

        //create correction block (on demand)
        if( _corr == null ){
            _corr = new MatrixBlock(value1.getNumRows(), value1.getNumColumns(), false);
        }

        //aggregate second input
        if(_op.correctionExists) {
            OperationsOnMatrixValues.incrementalAggregation(
                    out, _corr, value2, _op, true);
        }
        else {
            OperationsOnMatrixValues.incrementalAggregation(
                    out, null, value2, _op, true);
        }

        return out;
    }
}
