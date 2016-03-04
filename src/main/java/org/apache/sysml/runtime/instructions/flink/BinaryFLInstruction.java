package org.apache.sysml.runtime.instructions.flink;

import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.instructions.InstructionUtils;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.matrix.operators.Operator;

/**
 * Abstract class for all binary Flink instructions.
 */
public abstract class BinaryFLInstruction extends FLInstruction {

    protected CPOperand input1, input2, output;

    public BinaryFLInstruction(CPOperand input1, CPOperand input2, CPOperand output, String opcode, String istr) {
        super(opcode, istr);
        this.input1 = input1;
        this.input2 = input2;
        this.output = output;
    }

    public BinaryFLInstruction(Operator op, CPOperand input1, CPOperand input2, CPOperand output, String opcode, String istr) {
        super(op, opcode, istr);
        this.input1 = input1;
        this.input2 = input2;
        this.output = output;
    }

    /**
     *
     * @param instr
     * @param in1
     * @param in2
     * @param out
     * @return
     * @throws DMLRuntimeException
     */
    // TODO can potentially be moved to InstructionUtils
    protected static String parseBinaryInstruction(String instr, CPOperand in1, CPOperand in2, CPOperand out)
            throws DMLRuntimeException
    {
        String[] parts = InstructionUtils.getInstructionPartsWithValueType(instr);
        InstructionUtils.checkNumFields(parts, 3);

        String opcode = parts[0];
        in1.split(parts[1]);
        in2.split(parts[2]);
        out.split(parts[3]);

        return opcode;
    }
}
