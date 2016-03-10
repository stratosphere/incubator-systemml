package org.apache.sysml.runtime.instructions.flink;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.DMLUnsupportedOperationException;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.controlprogram.context.FlinkExecutionContext;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.operators.BinaryOperator;
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
        FlinkExecutionContext fec = (FlinkExecutionContext) ec;

        // check dimensions
        checkMatrixMatrixBinaryCharacteristics(fec);

        // get input
        String datasetVar1 = input1.getName();
        String datasetVar2 = input2.getName();
        DataSet<Tuple2<MatrixIndexes, MatrixBlock>> in1 = fec.getBinaryBlockDataSetHandleForVariable(datasetVar1);
        DataSet<Tuple2<MatrixIndexes, MatrixBlock>> in2 = fec.getBinaryBlockDataSetHandleForVariable(datasetVar2);
        MatrixCharacteristics mc1 = fec.getMatrixCharacteristics(datasetVar1);
        MatrixCharacteristics mc2 = fec.getMatrixCharacteristics(datasetVar2);

        BinaryOperator bop = (BinaryOperator) _optr;



    }

    protected void checkMatrixMatrixBinaryCharacteristics(FlinkExecutionContext fec)
            throws DMLRuntimeException
    {
        MatrixCharacteristics mc1 = fec.getMatrixCharacteristics(input1.getName());
        MatrixCharacteristics mc2 = fec.getMatrixCharacteristics(input2.getName());

        //check for unknown input dimensions
        if( !(mc1.dimsKnown() && mc2.dimsKnown()) ){
            throw new DMLRuntimeException("Unknown dimensions matrix-matrix binary operations: "
                    + "[" + mc1.getRows() + "x" + mc1.getCols()  + " vs " + mc2.getRows() + "x" + mc2.getCols() + "]");
        }

        //check for dimension mismatch
        if( (mc1.getRows() != mc2.getRows() ||  mc1.getCols() != mc2.getCols())
                && !(mc1.getRows() == mc2.getRows() && mc2.getCols()==1 ) //matrix-colvector
                && !(mc1.getCols() == mc2.getCols() && mc2.getRows()==1 ) //matrix-rowvector
                && !(mc1.getCols()==1 && mc2.getRows()==1) )     //outer colvector-rowvector
        {
            throw new DMLRuntimeException("Dimensions mismatch matrix-matrix binary operations: "
                    + "[" + mc1.getRows() + "x" + mc1.getCols()  + " vs " + mc2.getRows() + "x" + mc2.getCols() + "]");
        }

        if(mc1.getRowsPerBlock() != mc2.getRowsPerBlock() ||  mc1.getColsPerBlock() != mc2.getColsPerBlock()) {
            throw new DMLRuntimeException("Blocksize mismatch matrix-matrix binary operations: "
                    + "[" + mc1.getRowsPerBlock() + "x" + mc1.getColsPerBlock()  + " vs " + mc2.getRowsPerBlock() + "x" + mc2.getColsPerBlock() + "]");
        }
    }
}
