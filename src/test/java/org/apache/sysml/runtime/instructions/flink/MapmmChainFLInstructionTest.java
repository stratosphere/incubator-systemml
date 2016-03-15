package org.apache.sysml.runtime.instructions.flink;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.sysml.api.DMLScript;
import org.apache.sysml.lops.MapMultChain;
import org.apache.sysml.parser.Expression;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContextFactory;
import org.apache.sysml.runtime.controlprogram.context.FlinkExecutionContext;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.instructions.cp.VariableCPInstruction;
import org.apache.sysml.runtime.instructions.flink.utils.DataSetConverterUtils;
import org.apache.sysml.runtime.instructions.flink.utils.RowIndexedInputFormat;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.junit.Test;

public class MapmmChainFLInstructionTest {

    /**
     * Runs Flink instructions equivalent to
     *
     * <pre><code>
     *     X = read("flink/haberman.data", format="csv")
     *     v = read("flink/v.csv", format="csv")
     *
     *     C = t(X) %*% (X %*% v)
     *
     *     write(C, "/tmp/test.out", format="csv")
     * </code></pre>
     * @throws Exception
     */
    @Test
    public void testXtXvInstruction() throws Exception {
        String inputXFile = getClass().getClassLoader().getResource("flink/haberman.data").getFile();
        String inputVFile = getClass().getClassLoader().getResource("flink/v.csv").getFile();
        String outputFile = "/tmp/test.out";

        DMLScript.rtplatform = DMLScript.RUNTIME_PLATFORM.FLINK;
        FlinkExecutionContext fec = (FlinkExecutionContext) ExecutionContextFactory.createContext();

        VariableCPInstruction createvarX = VariableCPInstruction.parseInstruction(
                "CP°createvar°_mX°scratch_space//_p80815_141.23.124.66//_t0/temp3°true°binaryblock°306°4°1000°1000°-1");
        VariableCPInstruction createvarV = VariableCPInstruction.parseInstruction(
                "CP°createvar°_mv°scratch_space//_p80815_141.23.124.66//_t0/temp3°true°binaryblock°4°1°1000°1000°-1");
        VariableCPInstruction createvar3 = VariableCPInstruction.parseInstruction(
                "CP°createvar°_mVar3°scratch_space//_p80815_141.23.124.66//_t0/temp3°true°binaryblock°4°1°1000°1000°-1");
        MapmmChainFLInstruction mapmmChain = new MapmmChainFLInstruction(
                new CPOperand("_mX", Expression.ValueType.DOUBLE, Expression.DataType.MATRIX),
                new CPOperand("_mv", Expression.ValueType.DOUBLE, Expression.DataType.MATRIX),
                new CPOperand("_mVar3", Expression.ValueType.DOUBLE, Expression.DataType.MATRIX),
                MapMultChain.ChainType.XtXv,
                "%*%",
                "");
        // write to file
        WriteFLInstruction write = WriteFLInstruction.parseInstruction(
                "FLINK°write°_mVar3·MATRIX·DOUBLE°" + outputFile + "·SCALAR·STRING·true°csv·SCALAR·STRING·true°false°,°false°true");

        ExecutionEnvironment env = fec.getFlinkContext();

        DataSource<Tuple2<Integer, String>> xRaw = env.readFile(new RowIndexedInputFormat(), inputXFile);
        MatrixCharacteristics xMcOut = new MatrixCharacteristics(306, 4, 1, 4);
        DataSet<Tuple2<MatrixIndexes, MatrixBlock>> X = DataSetConverterUtils.csvToBinaryBlock(env, xRaw, xMcOut, false, ",", false, 0.0);
        DataSource<Tuple2<Integer, String>> vRaw = env.readFile(new RowIndexedInputFormat(), inputVFile);
        MatrixCharacteristics vMcOut = new MatrixCharacteristics(4, 1, 4, 1);
        DataSet<Tuple2<MatrixIndexes, MatrixBlock>> v = DataSetConverterUtils.csvToBinaryBlock(env, vRaw, vMcOut, false, ",", false, 0.0);

        createvarX.processInstruction(fec);
        createvarV.processInstruction(fec);
        fec.setDataSetHandleForVariable("_mX", X);
        fec.setDataSetHandleForVariable("_mv", v);
        createvar3.processInstruction(fec);
        mapmmChain.processInstruction(fec);
        write.processInstruction(fec);

        env.execute("Matrix multiplication chain (XtXv)");
    }

    /**
     * Runs Flink instructions equivalent to
     *
     * <pre><code>
     *     X = read("flink/haberman.data", format="csv")
     *     v = read("flink/v.csv", format="csv")
     *     w = read("flink/w.csv", format="csv")
     *
     *     C = t(X) %*% (w * (X %*% v))
     *
     *     write(C, "/tmp/test.out", format="csv")
     * </code></pre>
     * @throws Exception
     */
    @Test
    public void testXtwXvInstruction() throws Exception {
        String inputXFile = getClass().getClassLoader().getResource("flink/haberman.data").getFile();
        String inputVFile = getClass().getClassLoader().getResource("flink/v.csv").getFile();
        String inputWFile = getClass().getClassLoader().getResource("flink/w.csv").getFile();
        String outputFile = "/tmp/test.out";

        DMLScript.rtplatform = DMLScript.RUNTIME_PLATFORM.FLINK;
        FlinkExecutionContext fec = (FlinkExecutionContext) ExecutionContextFactory.createContext();

        VariableCPInstruction createvarX = VariableCPInstruction.parseInstruction(
                "CP°createvar°_mX°scratch_space//_p80815_141.23.124.66//_t0/temp3°true°binaryblock°306°4°1000°1000°-1");
        VariableCPInstruction createvarW = VariableCPInstruction.parseInstruction(
                "CP°createvar°_mv°scratch_space//_p80815_141.23.124.66//_t0/temp3°true°binaryblock°4°1°1000°1000°-1");
        VariableCPInstruction createvarV = VariableCPInstruction.parseInstruction(
                "CP°createvar°_mw°scratch_space//_p80815_141.23.124.66//_t0/temp3°true°binaryblock°306°1°1000°1000°-1");
        VariableCPInstruction createvar3 = VariableCPInstruction.parseInstruction(
                "CP°createvar°_mVar3°scratch_space//_p80815_141.23.124.66//_t0/temp3°true°binaryblock°4°1°1000°1000°-1");
        MapmmChainFLInstruction mapmmChain = new MapmmChainFLInstruction(
                new CPOperand("_mX", Expression.ValueType.DOUBLE, Expression.DataType.MATRIX),
                new CPOperand("_mv", Expression.ValueType.DOUBLE, Expression.DataType.MATRIX),
                new CPOperand("_mw", Expression.ValueType.DOUBLE, Expression.DataType.MATRIX),
                new CPOperand("_mVar3", Expression.ValueType.DOUBLE, Expression.DataType.MATRIX),
                MapMultChain.ChainType.XtwXv,
                "%*%",
                "");
        // write to file
        WriteFLInstruction write = WriteFLInstruction.parseInstruction(
                "FLINK°write°_mVar3·MATRIX·DOUBLE°" + outputFile + "·SCALAR·STRING·true°csv·SCALAR·STRING·true°false°,°false°true");

        ExecutionEnvironment env = fec.getFlinkContext();

        DataSource<Tuple2<Integer, String>> xRaw = env.readFile(new RowIndexedInputFormat(), inputXFile);
        MatrixCharacteristics xMcOut = new MatrixCharacteristics(306, 4, 1, 4);
        DataSet<Tuple2<MatrixIndexes, MatrixBlock>> X = DataSetConverterUtils.csvToBinaryBlock(env, xRaw, xMcOut, false, ",", false, 0.0);

        DataSource<Tuple2<Integer, String>> vRaw = env.readFile(new RowIndexedInputFormat(), inputVFile);
        MatrixCharacteristics vMcOut = new MatrixCharacteristics(4, 1, 4, 1);
        DataSet<Tuple2<MatrixIndexes, MatrixBlock>> v = DataSetConverterUtils.csvToBinaryBlock(env, vRaw, vMcOut, false, ",", false, 0.0);

        DataSource<Tuple2<Integer, String>> wRaw = env.readFile(new RowIndexedInputFormat(), inputWFile);
        MatrixCharacteristics wMcOut = new MatrixCharacteristics(306, 1, 1, 1);
        DataSet<Tuple2<MatrixIndexes, MatrixBlock>> w = DataSetConverterUtils.csvToBinaryBlock(env, wRaw, wMcOut, false, ",", false, 0.0);

        createvarX.processInstruction(fec);
        createvarV.processInstruction(fec);
        createvarW.processInstruction(fec);
        fec.setDataSetHandleForVariable("_mX", X);
        fec.setDataSetHandleForVariable("_mv", v);
        fec.setDataSetHandleForVariable("_mw", w);
        createvar3.processInstruction(fec);
        mapmmChain.processInstruction(fec);
        write.processInstruction(fec);

        env.execute("Matrix multiplication chain (XtwXv)");
    }

    /**
     * Runs Flink instructions equivalent to
     *
     * <pre><code>
     *     X = read("flink/haberman.data", format="csv")
     *     v = read("flink/v.csv", format="csv")
     *     w = read("flink/w.csv", format="csv")
     *
     *     C = t(X) %*% ((X %*% v) - w)
     *
     *     write(C, "/tmp/test.out", format="csv")
     * </code></pre>
     * @throws Exception
     */
    @Test
    public void testXtXvyInstruction() throws Exception {
        String inputXFile = getClass().getClassLoader().getResource("flink/haberman.data").getFile();
        String inputVFile = getClass().getClassLoader().getResource("flink/v.csv").getFile();
        String inputWFile = getClass().getClassLoader().getResource("flink/w.csv").getFile();
        String outputFile = "/tmp/test.out";

        DMLScript.rtplatform = DMLScript.RUNTIME_PLATFORM.FLINK;
        FlinkExecutionContext fec = (FlinkExecutionContext) ExecutionContextFactory.createContext();

        VariableCPInstruction createvarX = VariableCPInstruction.parseInstruction(
                "CP°createvar°_mX°scratch_space//_p80815_141.23.124.66//_t0/temp3°true°binaryblock°306°4°1000°1000°-1");
        VariableCPInstruction createvarW = VariableCPInstruction.parseInstruction(
                "CP°createvar°_mv°scratch_space//_p80815_141.23.124.66//_t0/temp3°true°binaryblock°4°1°1000°1000°-1");
        VariableCPInstruction createvarV = VariableCPInstruction.parseInstruction(
                "CP°createvar°_mw°scratch_space//_p80815_141.23.124.66//_t0/temp3°true°binaryblock°306°1°1000°1000°-1");
        VariableCPInstruction createvar3 = VariableCPInstruction.parseInstruction(
                "CP°createvar°_mVar3°scratch_space//_p80815_141.23.124.66//_t0/temp3°true°binaryblock°4°1°1000°1000°-1");
        MapmmChainFLInstruction mapmmChain = new MapmmChainFLInstruction(
                new CPOperand("_mX", Expression.ValueType.DOUBLE, Expression.DataType.MATRIX),
                new CPOperand("_mv", Expression.ValueType.DOUBLE, Expression.DataType.MATRIX),
                new CPOperand("_mw", Expression.ValueType.DOUBLE, Expression.DataType.MATRIX),
                new CPOperand("_mVar3", Expression.ValueType.DOUBLE, Expression.DataType.MATRIX),
                MapMultChain.ChainType.XtXvy,
                "%*%",
                "");
        // write to file
        WriteFLInstruction write = WriteFLInstruction.parseInstruction(
                "FLINK°write°_mVar3·MATRIX·DOUBLE°" + outputFile + "·SCALAR·STRING·true°csv·SCALAR·STRING·true°false°,°false°true");

        ExecutionEnvironment env = fec.getFlinkContext();

        DataSource<Tuple2<Integer, String>> xRaw = env.readFile(new RowIndexedInputFormat(), inputXFile);
        MatrixCharacteristics xMcOut = new MatrixCharacteristics(306, 4, 1, 4);
        DataSet<Tuple2<MatrixIndexes, MatrixBlock>> X = DataSetConverterUtils.csvToBinaryBlock(env, xRaw, xMcOut, false, ",", false, 0.0);

        DataSource<Tuple2<Integer, String>> vRaw = env.readFile(new RowIndexedInputFormat(), inputVFile);
        MatrixCharacteristics vMcOut = new MatrixCharacteristics(4, 1, 4, 1);
        DataSet<Tuple2<MatrixIndexes, MatrixBlock>> v = DataSetConverterUtils.csvToBinaryBlock(env, vRaw, vMcOut, false, ",", false, 0.0);

        DataSource<Tuple2<Integer, String>> wRaw = env.readFile(new RowIndexedInputFormat(), inputWFile);
        MatrixCharacteristics wMcOut = new MatrixCharacteristics(306, 1, 1, 1);
        DataSet<Tuple2<MatrixIndexes, MatrixBlock>> w = DataSetConverterUtils.csvToBinaryBlock(env, wRaw, wMcOut, false, ",", false, 0.0);

        createvarX.processInstruction(fec);
        createvarV.processInstruction(fec);
        createvarW.processInstruction(fec);
        fec.setDataSetHandleForVariable("_mX", X);
        fec.setDataSetHandleForVariable("_mv", v);
        fec.setDataSetHandleForVariable("_mw", w);
        createvar3.processInstruction(fec);
        mapmmChain.processInstruction(fec);
        write.processInstruction(fec);

        env.execute("Matrix multiplication chain (XtXvy)");
    }
}
