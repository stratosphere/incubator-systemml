package org.apache.sysml.runtime.instructions.flink.utils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.*;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;

/**
 * Created by fschueler on 22.02.16.
 */
public class ConverterTest {

    public static void main(String[] args) {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableForceKryo();

        DataSet<String> input = env.readTextFile("/home/fschueler/systemml-tutorial/data/haberman.data");
        int numRows = 306;
        int numCols = 4;
        int numRowsPerBlock = 10;
        int numColsPerBlock = 10;

        MatrixCharacteristics mcOut = new MatrixCharacteristics(numRows, numCols, numRowsPerBlock, numColsPerBlock);
        try {
            DataSet<Tuple2<MatrixIndexes, MatrixBlock>> mat = DatasetConverterUtils.csvToBinaryBlock(env, input, mcOut, false, ",", false, 0.0);
            mat.writeAsText("/tmp/flink");
            env.execute("lksadjf");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}
