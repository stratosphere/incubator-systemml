package org.apache.sysml.runtime.instructions.flink.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;

public class FilterDiagBlocksFunction implements FilterFunction<Tuple2<MatrixIndexes, MatrixBlock>> {

    @Override
    public boolean filter(Tuple2<MatrixIndexes, MatrixBlock> value) throws Exception {
        //returns true for matrix blocks on matrix diagonal
        MatrixIndexes ix = value.f0;
        return (ix.getRowIndex() == ix.getColumnIndex());
    }
}
