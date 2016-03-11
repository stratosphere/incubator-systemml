package org.apache.sysml.runtime.instructions.flink;

import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.DMLUnsupportedOperationException;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.matrix.operators.Operator;

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
        //sanity check opcode
        String opcode = getOpcode();
        if ( !(opcode.equalsIgnoreCase("+") || opcode.equalsIgnoreCase("-") || opcode.equalsIgnoreCase("*")
                || opcode.equalsIgnoreCase("/") || opcode.equalsIgnoreCase("%%") || opcode.equalsIgnoreCase("%/%")
                || opcode.equalsIgnoreCase("^") || opcode.equalsIgnoreCase("^2")
                || opcode.equalsIgnoreCase("*2") || opcode.equalsIgnoreCase("1-*")) )
        {
            throw new DMLRuntimeException("Unknown opcode in instruction: " + opcode);
        }

        super.processMatrixScalarBinaryInstruction(ec);
    }


}
