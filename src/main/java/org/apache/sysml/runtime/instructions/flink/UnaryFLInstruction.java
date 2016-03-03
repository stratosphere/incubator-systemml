package org.apache.sysml.runtime.instructions.flink;

import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.matrix.operators.Operator;

public abstract class UnaryFLInstruction extends ComputationFLInstruction {

    public UnaryFLInstruction(Operator op, CPOperand in, CPOperand out,
                              String opcode, String instr) {
        this (op, in, null, null, out, opcode, instr);
    }

    public UnaryFLInstruction(Operator op, CPOperand in1, CPOperand in2, CPOperand out,
                              String opcode, String instr) {
        this (op, in1, in2, null, out, opcode, instr);
    }

    public UnaryFLInstruction(Operator op, CPOperand in1, CPOperand in2, CPOperand in3, CPOperand out,
                              String opcode, String instr) {
        super(op, in1, in2, in3, out, opcode, instr);
    }

}
