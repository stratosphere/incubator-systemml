package org.apache.sysml.runtime.instructions.flink;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.sysml.lops.MMTSJ;
import org.apache.sysml.parser.Expression;
import org.apache.sysml.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContextFactory;
import org.apache.sysml.runtime.controlprogram.context.FlinkExecutionContext;
import org.apache.sysml.runtime.instructions.flink.data.DataSetObject;
import org.apache.sysml.runtime.instructions.flink.utils.DataSetConverterUtils;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.MatrixDimensionsMetaData;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.junit.Test;

public class TsmmFLInstructionTest {
    @Test
    public void testTSMM() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String testFile = getClass().getClassLoader().getResource("flink/haberman.data").getFile();
        String outFile  = "/tmp/sysml";
        DataSet<String> input = env.readTextFile(testFile);
        int numRowsPerBlock = 10;
        int numColsPerBlock = 10;

        // get execution context
        FlinkExecutionContext flec = (FlinkExecutionContext) ExecutionContextFactory.createContext();

        // transform data into dataset with systemml binary format
        MatrixCharacteristics mcOut = new MatrixCharacteristics(0L, 0L, numRowsPerBlock, numColsPerBlock);
        DataSet<Tuple2<MatrixIndexes, MatrixBlock>> mat = DataSetConverterUtils.csvToBinaryBlock(env, input, mcOut, false, ",", false, 0.0);

        // register the data in the flinkExecutionContext
        DataSetObject m = new DataSetObject(mat, "_mVar1");
        //DataSetObject out = new DataSetObject(null, "_mVar2"); // temporary result of instruction
        MatrixObject mo1 = new MatrixObject(Expression.ValueType.DOUBLE, testFile);
        //MatrixObject mo2 = new MatrixObject(Expression.ValueType.DOUBLE, outFile);
        MatrixDimensionsMetaData meta1 = new MatrixDimensionsMetaData(mcOut);
        MatrixDimensionsMetaData meta2 = new MatrixDimensionsMetaData();
        mo1.setDataSetHandle(m);
        //mo2.setDataSetHandle(out);
        mo1.setMetaData(meta1);
        //mo2.setMetaData(meta2);
        mo1.setVarName("_mVar1");
        //mo2.setVarName("_mVar2");
        flec.setVariable("_mVar1", mo1);
        //flec.setVariable("_mVar2", mo2);

        // parse and execute instructin code
        String instCode = "FLINK°tsmm°_mVar1·MATRIX·DOUBLE°_mVar2·MATRIX·DOUBLE°LEFT";
        TsmmFLInstruction inst = TsmmFLInstruction.parseInstruction(instCode);
        MatrixBlock out = inst.processInstructionWReturn(flec);

        System.out.println(out);
    }

    @Test
    public void testTSMMSpark() {

    }
}
