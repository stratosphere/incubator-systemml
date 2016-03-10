package org.apache.sysml.runtime.instructions.flink;


import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.operators.Operator;

public abstract class ComputationFLInstruction extends FLInstruction {

    public CPOperand output;
    public CPOperand input1, input2, input3;

    public ComputationFLInstruction(Operator op, CPOperand in1, CPOperand in2, CPOperand out, String opcode, String istr) {
        super(op, opcode, istr);
        input1 = in1;
        input2 = in2;
        input3 = null;
        output = out;
    }

    public ComputationFLInstruction( Operator op, CPOperand in1, CPOperand in2, CPOperand in3, CPOperand out, String opcode, String istr) {
        super(op, opcode, istr);
        input1 = in1;
        input2 = in2;
        input3 = in3;
        output = out;
    }

    public String getOutputVariableName() { return output.getName(); }

    /**
     *
     * @param ec
     * @throws DMLRuntimeException
     */
    // TODO this is the same code as in ComputationSPInstruction --> move to common place
    protected void updateBinaryOutputMatrixCharacteristics(ExecutionContext ec)
            throws DMLRuntimeException
    {
        MatrixCharacteristics mcIn1 = ec.getMatrixCharacteristics(input1.getName());
        MatrixCharacteristics mcIn2 = ec.getMatrixCharacteristics(input2.getName());
        MatrixCharacteristics mcOut = ec.getMatrixCharacteristics(output.getName());
        boolean outer = (mcIn1.getRows()>1 && mcIn1.getCols()==1 && mcIn2.getRows()==1 && mcIn2.getCols()>1);

        if(!mcOut.dimsKnown()) {
            if(!mcIn1.dimsKnown())
                throw new DMLRuntimeException("The output dimensions are not specified and cannot be inferred from input:" + mcIn1.toString() + " " + mcIn2.toString() + " " + mcOut.toString());
            else if(outer)
                ec.getMatrixCharacteristics(output.getName()).set(mcIn1.getRows(), mcIn2.getCols(), mcIn1.getRowsPerBlock(), mcIn2.getColsPerBlock());
            else
                ec.getMatrixCharacteristics(output.getName()).set(mcIn1.getRows(), mcIn1.getCols(), mcIn1.getRowsPerBlock(), mcIn1.getRowsPerBlock());
        }
    }

}
