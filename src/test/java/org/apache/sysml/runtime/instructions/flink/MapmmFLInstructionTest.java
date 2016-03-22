package org.apache.sysml.runtime.instructions.flink;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.sysml.api.DMLScript;
import org.apache.sysml.parser.Expression;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContextFactory;
import org.apache.sysml.runtime.controlprogram.context.FlinkExecutionContext;
import org.apache.sysml.runtime.functionobjects.Multiply;
import org.apache.sysml.runtime.functionobjects.Plus;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.instructions.cp.VariableCPInstruction;
import org.apache.sysml.runtime.instructions.flink.utils.DataSetConverterUtils;
import org.apache.sysml.runtime.instructions.flink.utils.RowIndexedInputFormat;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.operators.AggregateBinaryOperator;
import org.apache.sysml.runtime.matrix.operators.AggregateOperator;
import org.junit.Test;

/**
 * ----GENERIC (lines 1-6) [recompile=true]
 * ------CP createvar _mVar1 scratch_space//_p85759_10.0.0.5//_t0/temp1 true binaryblock 5 3 1000 1000 -1
 * ------SPARK rand 5 3 1000 1000 2.0 2.0 1.0 -1 scratch_space/_p85759_10.0.0.5//_t0/ uniform 1.0 _mVar1.MATRIX.DOUBLE
 * ------CP createvar _mVar2 scratch_space//_p85759_10.0.0.5//_t0/temp2 true binaryblock 3 5 1000 1000 -1
 * ------SPARK rand 3 5 1000 1000 3.0 3.0 1.0 -1 scratch_space/_p85759_10.0.0.5//_t0/ uniform 1.0 _mVar2.MATRIX.DOUBLE
 * ------CP createvar _mVar3 scratch_space//_p85759_10.0.0.5//_t0/temp3 true binaryblock 5 5 1000 1000 -1
 * ------SPARK mapmm _mVar1.MATRIX.DOUBLE _mVar2.MATRIX.DOUBLE _mVar3.MATRIX.DOUBLE RIGHT true NONE
 * ------CP rmvar _mVar1
 * ------CP rmvar _mVar2
 * ------SPARK write _mVar3.MATRIX.DOUBLE /tmp/mm.out.SCALAR.STRING.true csv.SCALAR.STRING.true false , false true
 * ------CP rmvar _mVar3
 */
public class MapmmFLInstructionTest {

    @Test
    public void testInstruction() throws Exception {
        String inputAFile = getClass().getClassLoader().getResource("flink/3-2.data").getFile();;
        String inputBFile = getClass().getClassLoader().getResource("flink/2-3.data").getFile();;
        String outputFile = "/tmp/test.out";
        DMLScript.rtplatform = DMLScript.RUNTIME_PLATFORM.FLINK;
        FlinkExecutionContext fec = (FlinkExecutionContext) ExecutionContextFactory.createContext();

        VariableCPInstruction createvarA = VariableCPInstruction.parseInstruction(
                "CP°createvar°_mA°scratch_space//_p80815_141.23.124.66//_t0/temp3°true°binaryblock°3°2°1000°1000°-1°false");
        VariableCPInstruction createvarB = VariableCPInstruction.parseInstruction(
                "CP°createvar°_mB°scratch_space//_p80815_141.23.124.66//_t0/temp3°true°binaryblock°2°3°1000°1000°-1°false");
        VariableCPInstruction createvar3 = VariableCPInstruction.parseInstruction(
                "CP°createvar°_mVar3°scratch_space//_p80815_141.23.124.66//_t0/temp3°true°binaryblock°3°3°1000°1000°-1°false");
        AggregateOperator agg = new AggregateOperator(0, Plus.getPlusFnObject());
        AggregateBinaryOperator aggbin = new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), agg);
        MapmmFLInstruction mapMM = new MapmmFLInstruction(
                new CPOperand("_mA", Expression.ValueType.DOUBLE, Expression.DataType.MATRIX),
                new CPOperand("_mB", Expression.ValueType.DOUBLE, Expression.DataType.MATRIX),
                new CPOperand("_mVar3", Expression.ValueType.DOUBLE, Expression.DataType.MATRIX),
                "%*%",
                "");
        // write to file
        WriteFLInstruction write = WriteFLInstruction.parseInstruction(
                "FLINK°write°_mVar3·MATRIX·DOUBLE°" + outputFile + "·SCALAR·STRING·true°csv·SCALAR·STRING·true°false°,°false°true");

        ExecutionEnvironment env = fec.getFlinkContext();

        DataSource<Tuple2<Integer, String>> aRaw = env.readFile(new RowIndexedInputFormat(), inputAFile);
        MatrixCharacteristics aMcOut = new MatrixCharacteristics(3, 2, 3, 1);
        DataSet<Tuple2<MatrixIndexes, MatrixBlock>> A = DataSetConverterUtils.csvToBinaryBlock(env, aRaw, aMcOut, false, ",", false, 0.0);
        DataSource<Tuple2<Integer, String>> bRaw = env.readFile(new RowIndexedInputFormat(), inputBFile);
        MatrixCharacteristics bMcOut = new MatrixCharacteristics(2, 3, 1, 3);
        DataSet<Tuple2<MatrixIndexes, MatrixBlock>> B = DataSetConverterUtils.csvToBinaryBlock(env, bRaw, bMcOut, false, ",", false, 0.0);

        createvarA.processInstruction(fec);
        createvarB.processInstruction(fec);
        fec.setDataSetHandleForVariable("_mA", A);
        fec.setDataSetHandleForVariable("_mB", B);
        createvar3.processInstruction(fec);
        mapMM.processInstruction(fec);
        write.processInstruction(fec);

        env.execute("Matrix multiplication");
    }
}
