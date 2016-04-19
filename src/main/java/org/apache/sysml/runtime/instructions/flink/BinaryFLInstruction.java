/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sysml.runtime.instructions.flink;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.sysml.lops.BinaryM.VectorType;
import org.apache.sysml.parser.Expression;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.controlprogram.context.FlinkExecutionContext;
import org.apache.sysml.runtime.instructions.InstructionUtils;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.instructions.cp.ScalarObject;
import org.apache.sysml.runtime.instructions.flink.functions.MatrixMatrixBinaryOpFunction;
import org.apache.sysml.runtime.instructions.flink.functions.MatrixScalarFunction;
import org.apache.sysml.runtime.instructions.flink.functions.ReplicateVectorFunction;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.operators.BinaryOperator;
import org.apache.sysml.runtime.matrix.operators.Operator;
import org.apache.sysml.runtime.matrix.operators.ScalarOperator;

/**
 * Abstract class for all binary Flink instructions.
 */
public abstract class BinaryFLInstruction extends ComputationFLInstruction {

    public BinaryFLInstruction(Operator op, CPOperand in1, CPOperand in2, CPOperand out, String opcode, String istr) {
        super(op, in1, in2, out, opcode, istr);
    }

    public BinaryFLInstruction(Operator op, CPOperand in1, CPOperand in2, CPOperand in3, CPOperand out, String opcode,
                               String istr) {
        super(op, in1, in2, in3, out, opcode, istr);
    }

    /**
     * @param instr
     * @param in1
     * @param in2
     * @param out
     * @return
     * @throws DMLRuntimeException
     */
    // TODO can potentially be moved to InstructionUtils
    protected static String parseBinaryInstruction(String instr, CPOperand in1, CPOperand in2, CPOperand out)
            throws DMLRuntimeException {
        String[] parts = InstructionUtils.getInstructionPartsWithValueType(instr);

		System.out.println(parts[0]);
		
        InstructionUtils.checkNumFields(parts, 3);

        String opcode = parts[0];
        in1.split(parts[1]);
        in2.split(parts[2]);
        out.split(parts[3]);

        return opcode;
    }

    protected void processMatrixMatrixBinaryInstruction(ExecutionContext ec) throws DMLRuntimeException {
        FlinkExecutionContext fec = (FlinkExecutionContext) ec;

        checkMatrixMatrixBinaryCharacteristics(fec);

        String dataSetVar1 = input1.getName();
        String dataSetVar2 = input2.getName();
        DataSet<Tuple2<MatrixIndexes, MatrixBlock>> in1 = fec.getBinaryBlockDataSetHandleForVariable(dataSetVar1);
        DataSet<Tuple2<MatrixIndexes, MatrixBlock>> in2 = fec.getBinaryBlockDataSetHandleForVariable(dataSetVar2);
        MatrixCharacteristics mc1 = fec.getMatrixCharacteristics(dataSetVar1);
        MatrixCharacteristics mc2 = fec.getMatrixCharacteristics(dataSetVar1);

        final BinaryOperator bop = (BinaryOperator) _optr;

        //vector replication if required (mv or outer operations)
        boolean rowvector = (mc2.getRows() == 1 && mc1.getRows() > 1);
        long numRepLeft = getNumReplicas(mc1, mc2, true);
        long numRepRight = getNumReplicas(mc1, mc2, false);

        if (numRepLeft > 1) {
            in1 = in1.flatMap(new ReplicateVectorFunction(false, numRepLeft));
        }
        if (numRepRight > 1) {
            in2 = in2.flatMap(new ReplicateVectorFunction(rowvector, numRepRight));
        }

        // execute binary operation
        DataSet<Tuple2<MatrixIndexes, MatrixBlock>> out = in1
                .join(in2)
                .where(0)
                .equalTo(0)
                .with(new MatrixMatrixBinaryOpFunction(bop));

        // set output
        updateBinaryOutputMatrixCharacteristics(fec);
        fec.setDataSetHandleForVariable(output.getName(), out);
        fec.addLineageDataSet(output.getName(), dataSetVar1);
        fec.addLineageDataSet(output.getName(), dataSetVar2);
    }

    protected void processMatrixScalarBinaryInstruction(ExecutionContext ec) throws Exception {
        FlinkExecutionContext fec = (FlinkExecutionContext) ec;

        // get variable names
        String matrixVar = (input1.getDataType() == Expression.DataType.MATRIX) ? input1.getName() : input2.getName();

        CPOperand scalar = (input1.getDataType() == Expression.DataType.MATRIX) ? input2 : input1;
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
     * @param ec
     * @param vtype
     * @throws DMLRuntimeException
     */
    protected void processMatrixBVectorBinaryInstruction(ExecutionContext ec, VectorType vtype)
            throws DMLRuntimeException {
        throw new DMLRuntimeException("not supported yet");
    }

    /**
     * @param fec
     * @param nameIn
     * @param nameOut
     * @throws DMLRuntimeException
     */
    protected void updateUnaryOutputMatrixCharacteristics(FlinkExecutionContext fec, String nameIn, String nameOut)
            throws DMLRuntimeException {
        MatrixCharacteristics mc1 = fec.getMatrixCharacteristics(nameIn);
        MatrixCharacteristics mcOut = fec.getMatrixCharacteristics(nameOut);
        if (!mcOut.dimsKnown()) {
            if (!mc1.dimsKnown())
                throw new DMLRuntimeException(
                        "The output dimensions are not specified and cannot be inferred from input:" + mc1.toString() + " " + mcOut.toString());
            else
                mcOut.set(mc1.getRows(), mc1.getCols(), mc1.getRowsPerBlock(), mc1.getColsPerBlock());
        }
    }

    /**
     * @param ec
     * @throws DMLRuntimeException
     */
    // TODO this method is identical to the one used in BinarySPInstruction and should be pushed somewhere else!
    protected void checkMatrixMatrixBinaryCharacteristics(ExecutionContext ec)
            throws DMLRuntimeException {
        MatrixCharacteristics mc1 = ec.getMatrixCharacteristics(input1.getName());
        MatrixCharacteristics mc2 = ec.getMatrixCharacteristics(input2.getName());

        //check for unknown input dimensions
        if (!(mc1.dimsKnown() && mc2.dimsKnown())) {
            throw new DMLRuntimeException("Unknown dimensions matrix-matrix binary operations: "
                    + "[" + mc1.getRows() + "x" + mc1.getCols() + " vs " + mc2.getRows() + "x" + mc2.getCols() + "]");
        }

        //check for dimension mismatch
        if ((mc1.getRows() != mc2.getRows() || mc1.getCols() != mc2.getCols())
                && !(mc1.getRows() == mc2.getRows() && mc2.getCols() == 1) //matrix-colvector
                && !(mc1.getCols() == mc2.getCols() && mc2.getRows() == 1) //matrix-rowvector
                && !(mc1.getCols() == 1 && mc2.getRows() == 1))     //outer colvector-rowvector
        {
            throw new DMLRuntimeException("Dimensions mismatch matrix-matrix binary operations: "
                    + "[" + mc1.getRows() + "x" + mc1.getCols() + " vs " + mc2.getRows() + "x" + mc2.getCols() + "]");
        }

        if (mc1.getRowsPerBlock() != mc2.getRowsPerBlock() || mc1.getColsPerBlock() != mc2.getColsPerBlock()) {
            throw new DMLRuntimeException("Blocksize mismatch matrix-matrix binary operations: "
                    + "[" + mc1.getRowsPerBlock() + "x" + mc1.getColsPerBlock() + " vs " + mc2.getRowsPerBlock() + "x" + mc2.getColsPerBlock() + "]");
        }
    }


	/**
	 *
	 * @param flec
	 * @throws DMLRuntimeException
	 */
	protected void updateBinaryMMOutputMatrixCharacteristics(FlinkExecutionContext flec, boolean checkCommonDim)
		throws DMLRuntimeException
	{
		MatrixCharacteristics mc1 = flec.getMatrixCharacteristics(input1.getName());
		MatrixCharacteristics mc2 = flec.getMatrixCharacteristics(input2.getName());
		MatrixCharacteristics mcOut = flec.getMatrixCharacteristics(output.getName());
		if(!mcOut.dimsKnown()) {
			if( !mc1.dimsKnown() || !mc2.dimsKnown() )
				throw new DMLRuntimeException("The output dimensions are not specified and cannot be inferred from inputs.");
			else if(mc1.getRowsPerBlock() != mc2.getRowsPerBlock() || mc1.getColsPerBlock() != mc2.getColsPerBlock())
				throw new DMLRuntimeException("Incompatible block sizes for BinarySPInstruction.");
			else if(checkCommonDim && mc1.getCols() != mc2.getRows())
				throw new DMLRuntimeException("Incompatible dimensions for BinarySPInstruction");
			else {
				mcOut.set(mc1.getRows(), mc2.getCols(), mc1.getRowsPerBlock(), mc1.getColsPerBlock());
			}
		}
	}

    /**
     * @param mc1
     * @param mc2
     * @param left
     * @return
     */
    // TODO this is exactly the same as in BinarySPInstruction --> move to common point!
    protected long getNumReplicas(MatrixCharacteristics mc1, MatrixCharacteristics mc2, boolean left) {
        if (left) {
            if (mc1.getCols() == 1) //outer
                return (long) Math.ceil((double) mc2.getCols() / mc2.getColsPerBlock());
        } else {
            if (mc2.getRows() == 1 && mc1.getRows() > 1) //outer, row vector
                return (long) Math.ceil((double) mc1.getRows() / mc1.getRowsPerBlock());
            else if (mc2.getCols() == 1 && mc1.getCols() > 1) //col vector
                return (long) Math.ceil((double) mc1.getCols() / mc1.getColsPerBlock());
        }

        return 1; //matrix-matrix
    }
}
