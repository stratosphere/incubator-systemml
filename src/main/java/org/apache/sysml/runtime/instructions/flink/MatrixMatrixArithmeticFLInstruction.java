package org.apache.sysml.runtime.instructions.flink;

import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.DMLUnsupportedOperationException;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.matrix.operators.Operator;

public class MatrixMatrixArithmeticFLInstruction extends ArithmeticBinaryFLInstruction {

    public MatrixMatrixArithmeticFLInstruction(Operator op, CPOperand in1, CPOperand in2, CPOperand out, String opcode, String istr)
            throws DMLRuntimeException {
        super(op, in1, in2, out, opcode, istr);

        //sanity check opcodes
        if ( !(  opcode.equalsIgnoreCase("+") || opcode.equalsIgnoreCase("-") || opcode.equalsIgnoreCase("*")
                || opcode.equalsIgnoreCase("/") || opcode.equalsIgnoreCase("%%") || opcode.equalsIgnoreCase("%/%")
                || opcode.equalsIgnoreCase("^") || opcode.equalsIgnoreCase("1-*") ) )
        {
            throw new DMLRuntimeException("Unknown opcode in MatrixMatrixArithmeticFLInstruction: " + toString());
        }
    }

    @Override
    public void processInstruction(ExecutionContext ec) throws DMLRuntimeException, DMLUnsupportedOperationException {
        //common binary matrix-matrix process instruction
        super.processMatrixMatrixBinaryInstruction(ec);
    }
}
