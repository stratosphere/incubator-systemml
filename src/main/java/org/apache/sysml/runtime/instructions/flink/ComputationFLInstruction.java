package org.apache.sysml.runtime.instructions.flink;


import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.matrix.operators.Operator;

public class ComputationFLInstruction extends FLInstruction {

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


}
