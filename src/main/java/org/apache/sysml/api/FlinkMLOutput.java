package org.apache.sysml.api;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;

import java.util.HashMap;

public class FlinkMLOutput extends MLOutput {

    HashMap<String, DataSet<Tuple2<MatrixIndexes,MatrixBlock>>> _outputs;

    public FlinkMLOutput(HashMap<String, DataSet<Tuple2<MatrixIndexes,MatrixBlock>>> outputs, HashMap<String, MatrixCharacteristics> outMetadata) {
        super(outMetadata);
        this._outputs = outputs;
    }

    public DataSet<Tuple2<MatrixIndexes,MatrixBlock>> getBinaryBlockedDataSet(String varName) throws DMLRuntimeException {
        if(_outputs.containsKey(varName)) {
            return _outputs.get(varName);
        }
        throw new DMLRuntimeException("Variable " + varName + " not found in the output symbol table.");
    }

    // TODO this should be refactored (Superclass MLOutput with Spark and Flink specific subclasses...)
    @Override
    public JavaPairRDD<MatrixIndexes,MatrixBlock> getBinaryBlockedRDD(String varName) throws DMLRuntimeException {
        throw new DMLRuntimeException("FlinkOutput can't return Spark RDDs!");
    }
}
