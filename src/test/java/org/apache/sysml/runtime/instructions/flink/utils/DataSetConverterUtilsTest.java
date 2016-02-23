package org.apache.sysml.runtime.instructions.flink.utils;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class DataSetConverterUtilsTest {

    @Test
    public void testCsvToBinaryBlock() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String testFile = getClass().getClassLoader().getResource("flink/haberman.data").getFile();
        DataSet<String> input = env.readTextFile(testFile);
        int numRowsPerBlock = 10;
        int numColsPerBlock = 10;

        MatrixCharacteristics mcOut = new MatrixCharacteristics(0L, 0L, numRowsPerBlock, numColsPerBlock);
        DataSet<Tuple2<MatrixIndexes, MatrixBlock>> mat = DataSetConverterUtils.csvToBinaryBlock(env, input, mcOut, false, ",", false, 0.0);

        List<Tuple2<MatrixIndexes, MatrixBlock>> blocks = mat.collect();

        assertEquals(31,blocks.size());
        assertEquals(4, mcOut.getCols());
        assertEquals(306, mcOut.getRows());
        assertEquals(1088, mcOut.getNonZeros());
    }
}