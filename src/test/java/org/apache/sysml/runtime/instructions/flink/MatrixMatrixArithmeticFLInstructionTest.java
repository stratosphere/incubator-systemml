package org.apache.sysml.runtime.instructions.flink;

import org.apache.sysml.api.DMLScript;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContextFactory;
import org.apache.sysml.runtime.controlprogram.context.FlinkExecutionContext;
import org.apache.sysml.runtime.instructions.cp.VariableCPInstruction;
import org.junit.Test;

/**
 * Tests a SystemML program given by the following DML script:
 *
 * <pre><code>
 * m = read("[input dataset]", rows=306, cols=4)
 * m3 = m * m
 * write(m3, "[output dataset]", format="csv")
 * </pre></code>
 */
public class MatrixMatrixArithmeticFLInstructionTest {

    @Test
    public void testInstruction() throws Exception {
        String inputFile = getClass().getClassLoader().getResource("flink/haberman.data").getFile();
        String outputFile = "/tmp/" + MatrixScalarArithmeticFLInstructionTest.class.getSimpleName() + "/testInstruction.out";
        DMLScript.rtplatform = DMLScript.RUNTIME_PLATFORM.FLINK;
        FlinkExecutionContext context = (FlinkExecutionContext) ExecutionContextFactory.createContext();

        // create variable with path to the csv
        VariableCPInstruction pREADm = VariableCPInstruction.parseInstruction(
                "CP°createvar°pREADm°" + inputFile + "°false°csv°306°4°-1°-1°-1°false°false°,°true°0.0");
        // create variable Var1
        VariableCPInstruction _mVar1 = VariableCPInstruction.parseInstruction(
                "CP°createvar°_mVar1°scratch_space//_p80815_141.23.124.66//_t0/temp1°true°binarycell°306°4°-1°-1°-1°false");

        // reblock - read file into Var1 DataSet<Tuple2<MatrixIndexes, MatrixBlock>>
        ReblockFLInstruction rblk = ReblockFLInstruction.parseInstruction(
                "FLINK°rblk°pREADm·MATRIX·DOUBLE°_mVar1·MATRIX·DOUBLE°1000°1000°true");
        VariableCPInstruction createvar2 = VariableCPInstruction.parseInstruction(
                "CP°createvar°_mVar2°scratch_space//_p80815_141.23.124.66//_t0/temp2°true°binaryblock°306°4°1000°1000°-1°false");
        CheckpointFLInstruction chkpnt = CheckpointFLInstruction.parseInstruction(
                "FLINK°chkpoint°_mVar1·MATRIX·DOUBLE°_mVar2·MATRIX·DOUBLE°MEMORY_AND_DISK");
        VariableCPInstruction rmVar1 = VariableCPInstruction.parseInstruction(
                "CP°rmvar°_mVar1");

        // create output variable Var3
        VariableCPInstruction createvar3 = VariableCPInstruction.parseInstruction(
                "CP°createvar°_mVar3°scratch_space//_p80815_141.23.124.66//_t0/temp3°true°binaryblock°306°4°1000°1000°-1°false");

        // multiply with scalar and write result to Var3
        // this is the instruction under test!
        MatrixMatrixArithmeticFLInstruction mult = (MatrixMatrixArithmeticFLInstruction) ArithmeticBinaryFLInstruction.parseInstruction(
                "FLINK°*°_mVar2·MATRIX·DOUBLE°_mVar2·MATRIX·DOUBLE·true°_mVar3·MATRIX·DOUBLE");
        VariableCPInstruction rmVar2 = VariableCPInstruction.parseInstruction(
                "CP°rmvar°_mVar2");

        // write to file
        WriteFLInstruction write = WriteFLInstruction.parseInstruction(
                "FLINK°write°_mVar3·MATRIX·DOUBLE°" + outputFile + "·SCALAR·STRING·true°csv·SCALAR·STRING·true°false°,°false°true");

        // delete the vars
        VariableCPInstruction rmVar3 = VariableCPInstruction.parseInstruction(
                "CP°rmvar°_mVar3");

        pREADm.processInstruction(context);
        _mVar1.processInstruction(context);
        rblk.processInstruction(context);
        createvar2.processInstruction(context);
        chkpnt.processInstruction(context);
        rmVar1.processInstruction(context);
        createvar3.processInstruction(context);
        mult.processInstruction(context);
        write.processInstruction(context);
        rmVar2.processInstruction(context);
        rmVar3.processInstruction(context);

        context.getFlinkContext().execute("SystemML");
    }
}
