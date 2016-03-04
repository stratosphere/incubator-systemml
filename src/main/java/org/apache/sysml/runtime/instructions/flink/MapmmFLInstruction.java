package org.apache.sysml.runtime.instructions.flink;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.sysml.parser.Expression;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.DMLUnsupportedOperationException;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.controlprogram.context.FlinkExecutionContext;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;

import static org.apache.sysml.runtime.instructions.flink.functions.MatrixMultiplicationFunctions.*;

/**
 * Implementation of {@link org.apache.sysml.runtime.instructions.Instruction} to run matrix multiplication on Flink.
 */
public class MapmmFLInstruction extends BinaryFLInstruction {

    public MapmmFLInstruction(CPOperand input1, CPOperand input2, CPOperand output, String opcode, String istr) {
        super(input1, input2, output, opcode, istr);
    }

    /**
     * Factory method that parses an instruction string of the form
     *
     * <pre><code>FLINK°mapmm°_mVar1·MATRIX·DOUBLE°_mVar2·MATRIX·DOUBLE°_mVar3·MATRIX·DOUBLE</code></pre>
     *
     * and returns a new {@link MapmmFLInstruction} representing the instruction string.
     * @param instr Instruction string, operands separated by <code>°</code> and value types by <code>·</code>
     * @return a new MapmmFLInstruction
     * @throws DMLRuntimeException
     * @throws DMLUnsupportedOperationException
     */
    public static MapmmFLInstruction parseInstruction(String instr) throws DMLRuntimeException, DMLUnsupportedOperationException {
        CPOperand input1 = new CPOperand("", Expression.ValueType.UNKNOWN, Expression.DataType.UNKNOWN);
        CPOperand input2 = new CPOperand("", Expression.ValueType.UNKNOWN, Expression.DataType.UNKNOWN);
        CPOperand output = new CPOperand("", Expression.ValueType.UNKNOWN, Expression.DataType.UNKNOWN);
        String opCode = parseBinaryInstruction(instr, input1, input2, output);
        if (!"mapMM".equalsIgnoreCase(opCode)) {
            throw new DMLUnsupportedOperationException("Unsupported operation with opcode " + opCode);
        }

        return new MapmmFLInstruction(input1, input2, output, opCode, instr);
    }

    @Override
    public void processInstruction(ExecutionContext ec) throws DMLRuntimeException, DMLUnsupportedOperationException {
        assert ec instanceof FlinkExecutionContext :
                "Expected " + FlinkExecutionContext.class.getCanonicalName() + " got " + ec.getClass().getCanonicalName();
        FlinkExecutionContext fec = (FlinkExecutionContext) ec;

        DataSet<Tuple2<MatrixIndexes, MatrixBlock>> A = fec.getBinaryBlockDataSetHandleForVariable(input1.getName());
        DataSet<Tuple2<MatrixIndexes, MatrixBlock>> B = fec.getBinaryBlockDataSetHandleForVariable(input2.getName());
        DataSet<Tuple2<MatrixIndexes, MatrixBlock>> out = A
                .join(B)
                .where(new ColumnSelector())
                .equalTo(new RowSelector())
                .with(new MultiplyMatrixBlocks())
                .groupBy(new MatrixIndexesSelector())
                .combineGroup(new SumMatrixBlocksCombine())
                .reduce(new SumMatrixBlocks());

        // register variable for output
        fec.setDataSetHandleForVariable(output.getName(), out);
    }
}
