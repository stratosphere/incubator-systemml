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
import org.apache.sysml.parser.Expression;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.controlprogram.context.FlinkExecutionContext;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.instructions.flink.functions.MatrixMultiplicationFunctions;
import org.apache.sysml.runtime.instructions.spark.BinarySPInstruction;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;

/**
 * Cpmm: cross-product matrix multiplication operation (distributed matrix multiply
 * by join over common dimension and subsequent aggregation of partial results).
 * <p>
 * NOTE: There is additional optimization potential by preventing aggregation for a single
 * block on the common dimension. However, in such a case we would never pick cpmm because
 * this would result in a degree of parallelism of 1.
 */
public class CpmmFLInstruction extends BinarySPInstruction {

    public CpmmFLInstruction(CPOperand input1, CPOperand input2, CPOperand output, String opcode, String istr) {
        super(null, input1, input2, output, opcode, istr);
    }

    /**
     * Factory method that parses an instruction string of the form
     * <p>
     * <pre><code>FLINK°cpmm°_mVar1·MATRIX·DOUBLE°_mVar2·MATRIX·DOUBLE°_mVar3·MATRIX·DOUBLE</code></pre>
     *
     * and returns a new {@link CpmmFLInstruction} representing the instruction string.
     *
     * @param instr Instruction string, operands separated by <code>°</code> and value types by <code>·</code>
     * @return a new CpmmFLInstruction
     * @throws DMLRuntimeException
     */
    public static CpmmFLInstruction parseInstruction(String instr)
            throws DMLRuntimeException {
        CPOperand input1 = new CPOperand("", Expression.ValueType.UNKNOWN, Expression.DataType.UNKNOWN);
        CPOperand input2 = new CPOperand("", Expression.ValueType.UNKNOWN, Expression.DataType.UNKNOWN);
        CPOperand output = new CPOperand("", Expression.ValueType.UNKNOWN, Expression.DataType.UNKNOWN);
        String opCode = parseBinaryInstruction(instr, input1, input2, output);
        if (!"CPMM".equalsIgnoreCase(opCode)) {
            throw new DMLRuntimeException("Unsupported operation with opcode " + opCode);
        }

        return new CpmmFLInstruction(input1, input2, output, opCode, instr);
    }

    @Override
    public void processInstruction(ExecutionContext ec) throws DMLRuntimeException {
        assert ec instanceof FlinkExecutionContext :
                "Expected " + FlinkExecutionContext.class.getCanonicalName() + " got " + ec.getClass().getCanonicalName();
        FlinkExecutionContext fec = (FlinkExecutionContext) ec;

        DataSet<Tuple2<MatrixIndexes, MatrixBlock>> A = fec.getBinaryBlockDataSetHandleForVariable(input1.getName());
        DataSet<Tuple2<MatrixIndexes, MatrixBlock>> B = fec.getBinaryBlockDataSetHandleForVariable(input2.getName());
        DataSet<Tuple2<MatrixIndexes, MatrixBlock>> out = A
                .join(B)
                .where(new MatrixMultiplicationFunctions.ColumnSelector())
                .equalTo(new MatrixMultiplicationFunctions.RowSelector())
                .with(new MatrixMultiplicationFunctions.MultiplyMatrixBlocks())
                .groupBy(new MatrixMultiplicationFunctions.MatrixIndexesSelector())
                .combineGroup(new MatrixMultiplicationFunctions.SumMatrixBlocksCombine())
                .reduce(new MatrixMultiplicationFunctions.SumMatrixBlocksStable());

        // register variable for output
        fec.setDataSetHandleForVariable(output.getName(), out);
    }
}
