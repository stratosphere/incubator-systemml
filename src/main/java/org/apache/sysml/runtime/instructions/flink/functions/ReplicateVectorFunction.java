package org.apache.sysml.runtime.instructions.flink.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;

/**
 * Replicates a Vector row- or column-wise
 */
public class ReplicateVectorFunction implements FlatMapFunction<Tuple2<MatrixIndexes, MatrixBlock>, Tuple2<MatrixIndexes, MatrixBlock>> {
    private static final long serialVersionUID = -1505557561471236851L;

    private boolean _byRow;
    private long _numReplicas;

    public ReplicateVectorFunction(boolean byRow, long numReplicas)
    {
        _byRow = byRow;
        _numReplicas = numReplicas;
    }

    @Override
    public void flatMap(Tuple2<MatrixIndexes, MatrixBlock> value, Collector<Tuple2<MatrixIndexes, MatrixBlock>> out) throws Exception {
        MatrixIndexes ix = value.f0;
        MatrixBlock mb = value.f1;

        //sanity check inputs
        if(_byRow && (ix.getRowIndex() != 1 || mb.getNumRows()>1) ) {
            throw new Exception("Expected a row vector in ReplicateVector");
        }
        if(!_byRow && (ix.getColumnIndex() != 1 || mb.getNumColumns()>1) ) {
            throw new Exception("Expected a column vector in ReplicateVector");
        }

        for (int i = 1; i <= _numReplicas; i++) {
            if (_byRow) {
                out.collect(new Tuple2<MatrixIndexes, MatrixBlock>(new MatrixIndexes(i, ix.getColumnIndex()), mb));
            } else {
                out.collect(new Tuple2<MatrixIndexes, MatrixBlock>(new MatrixIndexes(ix.getRowIndex(), i), mb));
            }
        }

        out.close();
    }
}
