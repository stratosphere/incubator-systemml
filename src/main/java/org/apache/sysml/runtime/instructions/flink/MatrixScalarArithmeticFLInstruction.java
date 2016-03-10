package org.apache.sysml.runtime.instructions.flink;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.sysml.parser.Expression;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.DMLUnsupportedOperationException;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.controlprogram.context.FlinkExecutionContext;
import org.apache.sysml.runtime.instructions.InstructionUtils;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.instructions.cp.ScalarObject;
import org.apache.sysml.runtime.instructions.flink.functions.MatrixScalarFunction;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.operators.Operator;
import org.apache.sysml.runtime.matrix.operators.ScalarOperator;

/**
 * Flink instruction for operation on a matrix and a scalar.
 *<br/>
 * The scalar operation is applied element-wise to all elements of the matrix.
 * <br/>
 * The instruction has the following structure
 * <pre><code>FLINK°[OPCODE]°[LEFT OPERAND]°[RIGHT OPERAND]°[OUTPUT]</code></pre>
 * where <code>[LEFT OPERAND] is a matrix and [RIGHT OPERAND] is a scalar, or vice versa.</code>
 */
public class MatrixScalarArithmeticFLInstruction extends ArithmeticBinaryFLInstruction {

    public MatrixScalarArithmeticFLInstruction(Operator op, CPOperand input1, CPOperand input2, CPOperand output, String opcode, String istr) {
        super(op, input1, input2, output, opcode, istr);
    }

    @Override
    public void processInstruction(ExecutionContext ec) throws DMLRuntimeException, DMLUnsupportedOperationException {
        FlinkExecutionContext fec = (FlinkExecutionContext) ec;

        // get variable names
        String matrixVar = (input1.getDataType() == Expression.DataType.MATRIX) ? input1.getName() : input2.getName();

        CPOperand scalar = ( input1.getDataType() == Expression.DataType.MATRIX ) ? input2 : input1;
        ScalarObject constant = ec.getScalarInput(scalar.getName(), scalar.getValueType(), scalar.isLiteral());
        ScalarOperator sc_op = (ScalarOperator) _optr;
        sc_op.setConstant(constant.getDoubleValue());

        //get input
        DataSet<Tuple2<MatrixIndexes, MatrixBlock>> in = fec.getBinaryBlockDataSetHandleForVariable(matrixVar);
        // apply scalar function element-wise
        DataSet<Tuple2<MatrixIndexes, MatrixBlock>> out = in.map(new MatrixScalarFunction(sc_op));

        // update the MatrixCharacteristics (important when number of 0 changes)
        updateUnaryOutputMatrixCharacteristics(fec, matrixVar, output.getName());

        // register variable for output
        fec.setDataSetHandleForVariable(output.getName(), out);
        fec.addLineageDataSet(output.getName(), matrixVar);
    }

    /**
     *
     * @param fec
     * @param nameIn
     * @param nameOut
     * @throws DMLRuntimeException
     */
    protected void updateUnaryOutputMatrixCharacteristics(FlinkExecutionContext fec, String nameIn, String nameOut)
            throws DMLRuntimeException
    {
        MatrixCharacteristics mc1 = fec.getMatrixCharacteristics(nameIn);
        MatrixCharacteristics mcOut = fec.getMatrixCharacteristics(nameOut);
        if(!mcOut.dimsKnown()) {
            if(!mc1.dimsKnown())
                throw new DMLRuntimeException("The output dimensions are not specified and cannot be inferred from input:" + mc1.toString() + " " + mcOut.toString());
            else
                mcOut.set(mc1.getRows(), mc1.getCols(), mc1.getRowsPerBlock(), mc1.getColsPerBlock());
        }
    }
}
