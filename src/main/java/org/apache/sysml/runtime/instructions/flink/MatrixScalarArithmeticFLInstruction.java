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

import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.matrix.operators.Operator;

/**
 * Flink instruction for operation on a matrix and a scalar.
 * <br/>
 * The scalar operation is applied element-wise to all elements of the matrix.
 * <br/>
 * The instruction has the following structure
 * <pre><code>FLINK°[OPCODE]°[LEFT OPERAND]°[RIGHT OPERAND]°[OUTPUT]</code></pre>
 * where <code>[LEFT OPERAND] is a matrix and [RIGHT OPERAND] is a scalar, or vice versa.</code>
 */
public class MatrixScalarArithmeticFLInstruction extends ArithmeticBinaryFLInstruction {

    public MatrixScalarArithmeticFLInstruction(Operator op, CPOperand input1, CPOperand input2, CPOperand output,
                                               String opcode, String istr) {
        super(op, input1, input2, output, opcode, istr);
    }

    @Override
    public void processInstruction(ExecutionContext ec) throws DMLRuntimeException {
        //sanity check opcode
        String opcode = getOpcode();
        if (!(opcode.equalsIgnoreCase("+") || opcode.equalsIgnoreCase("-") || opcode.equalsIgnoreCase("*")
                || opcode.equalsIgnoreCase("/") || opcode.equalsIgnoreCase("%%") || opcode.equalsIgnoreCase("%/%")
                || opcode.equalsIgnoreCase("^") || opcode.equalsIgnoreCase("^2")
                || opcode.equalsIgnoreCase("*2") || opcode.equalsIgnoreCase("1-*"))) {
            throw new DMLRuntimeException("Unknown opcode in instruction: " + opcode);
        }

        //common binary matrix-scalar process instruction
        super.processMatrixScalarBinaryInstruction(ec);
    }


}
