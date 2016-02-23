package org.apache.sysml.runtime.instructions.flink;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.sysml.lops.MMTSJ;
import org.apache.sysml.runtime.instructions.flink.utils.DataSetConverterUtils;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.junit.Test;

public class TsmmFLInstructionTest {
    @Test
    public void testTSMM() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String testFile = getClass().getClassLoader().getResource("flink/haberman.data").getFile();
        DataSet<String> input = env.readTextFile(testFile);
        int numRowsPerBlock = 10;
        int numColsPerBlock = 10;

        MatrixCharacteristics mcOut = new MatrixCharacteristics(0L, 0L, numRowsPerBlock, numColsPerBlock);
        DataSet<Tuple2<MatrixIndexes, MatrixBlock>> mat = DataSetConverterUtils.csvToBinaryBlock(env, input, mcOut, false, ",", false, 0.0);

        TsmmFLInstruction inst = new TsmmFLInstruction();
        MatrixBlock block = inst.processInstruction(mat);

        System.out.println(block.toString());
    }
}
