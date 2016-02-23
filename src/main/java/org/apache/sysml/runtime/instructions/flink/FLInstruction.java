package org.apache.sysml.runtime.instructions.flink;

import org.apache.sysml.lops.runtime.RunMRJobs;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.DMLUnsupportedOperationException;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.instructions.FLInstructionParser;
import org.apache.sysml.runtime.instructions.Instruction;
import org.apache.sysml.runtime.matrix.operators.Operator;

public class FLInstruction extends Instruction {

    public enum FLINSTRUCTION_TYPE {
        TSMM, // MAPMM, MAPMMCHAIN, CPMM, RMM, PMM, ZIPMM, PMAPMM, //matrix multiplication instructions
//        MatrixIndexing, Reorg, ArithmeticBinary, RelationalBinary, AggregateUnary, AggregateTernary, Reblock, CSVReblock,
//        Builtin, BuiltinUnary, BuiltinBinary, Checkpoint,
//        CentralMoment, Covariance, QSort, QPick,
//        ParameterizedBuiltin, MAppend, RAppend, GAppend, GAlignedAppend, Rand,
//        MatrixReshape, Ternary, Quaternary, CumsumAggregate, CumsumOffset, BinUaggChain, UaggOuterChain,
//        Write, INVALID,
    };

    protected FLINSTRUCTION_TYPE _fltype;
    protected Operator _optr;

    protected boolean _requiresLabelUpdate = false;

    public FLInstruction(String opcode, String istr) {
        type = INSTRUCTION_TYPE.FLINK;
        instString = istr;
        instOpcode = opcode;

        //update requirement for repeated usage
        _requiresLabelUpdate = super.requiresLabelUpdate();
    }

    public FLInstruction(Operator op, String opcode, String istr) {
        this(opcode, istr);
        _optr = op;
    }

    public FLINSTRUCTION_TYPE getFLInstructionType() { return _fltype; }

    @Override
    public boolean requiresLabelUpdate()
    {
        return _requiresLabelUpdate;
    }

    @Override
    public String getGraphString() {
        return getOpcode();
    }

    @Override
    public Instruction preprocessInstruction(ExecutionContext ec)
        throws DMLRuntimeException, DMLUnsupportedOperationException
    {
        //default pre-process behavior (e.g., debug state)
        Instruction tmp = super.preprocessInstruction(ec);

        //instruction patching
        if( tmp.requiresLabelUpdate() ) //update labels only if required
        {
            //note: no exchange of updated instruction as labels might change in the general case
            String updInst = RunMRJobs.updateLabels(tmp.toString(), ec.getVariables());
            tmp = FLInstructionParser.parseSingleInstruction(updInst);
        }

        return tmp;
    }

    @Override
    public void processInstruction(ExecutionContext ec) throws DMLRuntimeException, DMLUnsupportedOperationException {

    }
}
