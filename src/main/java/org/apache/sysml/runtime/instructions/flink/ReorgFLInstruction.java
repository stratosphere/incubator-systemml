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
import org.apache.sysml.runtime.functionobjects.DiagIndex;
import org.apache.sysml.runtime.functionobjects.RevIndex;
import org.apache.sysml.runtime.functionobjects.SortIndex;
import org.apache.sysml.runtime.functionobjects.SwapIndex;
import org.apache.sysml.runtime.instructions.InstructionUtils;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.instructions.flink.functions.ReorgMapFunction;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.operators.Operator;
import org.apache.sysml.runtime.matrix.operators.ReorgOperator;

public class ReorgFLInstruction extends UnaryFLInstruction {

        //sort-specific attributes (to enable variable attributes)
        private CPOperand _col = null;
        private CPOperand _desc = null;
        private CPOperand _ixret = null;
        private boolean _bSortIndInMem = false;

        public ReorgFLInstruction(Operator op, CPOperand in, CPOperand out, String opcode, String istr){
            super(op, in, out, opcode, istr);
            _fltype = FLINSTRUCTION_TYPE.Reorg;
        }

        public ReorgFLInstruction(Operator op, CPOperand in, CPOperand col, CPOperand desc, CPOperand ixret, CPOperand out, String opcode, boolean bSortIndInMem, String istr){
            this(op, in, out, opcode, istr);
            _col = col;
            _desc = desc;
            _ixret = ixret;
            _fltype = FLINSTRUCTION_TYPE.Reorg;
            _bSortIndInMem = bSortIndInMem;
        }

        public static ReorgFLInstruction parseInstruction ( String str )
                throws DMLRuntimeException
        {
            CPOperand in = new CPOperand("", Expression.ValueType.UNKNOWN, Expression.DataType.UNKNOWN);
            CPOperand out = new CPOperand("", Expression.ValueType.UNKNOWN, Expression.DataType.UNKNOWN);
            String opcode = InstructionUtils.getOpCode(str);

            if ( opcode.equalsIgnoreCase("r'") ) {
                parseUnaryInstruction(str, in, out); //max 2 operands
                return new ReorgFLInstruction(new ReorgOperator(SwapIndex.getSwapIndexFnObject()), in, out, opcode, str);
            }
            else if ( opcode.equalsIgnoreCase("rev") ) {
                parseUnaryInstruction(str, in, out); //max 2 operands
                return new ReorgFLInstruction(new ReorgOperator(RevIndex.getRevIndexFnObject()), in, out, opcode, str);
            }
            else if ( opcode.equalsIgnoreCase("rdiag") ) {
                parseUnaryInstruction(str, in, out); //max 2 operands
                return new ReorgFLInstruction(new ReorgOperator(DiagIndex.getDiagIndexFnObject()), in, out, opcode, str);
            }
            else if ( opcode.equalsIgnoreCase("rsort") ) {
                String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
                InstructionUtils.checkNumFields(parts, 5, 6);
                in.split(parts[1]);
                out.split(parts[5]);
                CPOperand col = new CPOperand(parts[2]);
                CPOperand desc = new CPOperand(parts[3]);
                CPOperand ixret = new CPOperand(parts[4]);
                boolean bSortIndInMem = false;

                if(parts.length > 5)
                    bSortIndInMem = Boolean.parseBoolean(parts[6]);

                return new ReorgFLInstruction(new ReorgOperator(SortIndex.getSortIndexFnObject(1,false,false)),
                        in, col, desc, ixret, out, opcode, bSortIndInMem, str);
            }
            else {
                throw new DMLRuntimeException("Unknown opcode while parsing a ReorgInstruction: " + str);
            }
        }

        @Override
        public void processInstruction(ExecutionContext ec)
                throws DMLRuntimeException
        {
            FlinkExecutionContext sec = (FlinkExecutionContext) ec;
            String opcode = getOpcode();

            //get input dataset handle
            DataSet<Tuple2<MatrixIndexes,MatrixBlock>> in1 = sec.getBinaryBlockDataSetHandleForVariable( input1.getName() );
            DataSet<Tuple2<MatrixIndexes,MatrixBlock>> out = null;
            MatrixCharacteristics mcIn = sec.getMatrixCharacteristics(input1.getName());

            if( opcode.equalsIgnoreCase("r'") ) //TRANSPOSE
            {
                //execute transpose reorg operation
                out = in1.map(new ReorgMapFunction(opcode));
            }
            else if( opcode.equalsIgnoreCase("rev") ) //REVERSE
            {
                throw new UnsupportedOperationException();
                //execute reverse reorg operation
            }
            else if ( opcode.equalsIgnoreCase("rdiag") ) // DIAG
            {
                throw new UnsupportedOperationException();
            }
            else if ( opcode.equalsIgnoreCase("rsort") ) //ORDER
            {
                throw new UnsupportedOperationException();
            }
            else {
                throw new DMLRuntimeException("Error: Incorrect opcode in ReorgFLInstruction:" + opcode);
            }

            //store output dataset handle
            updateReorgMatrixCharacteristics(sec);
            sec.setDataSetHandleForVariable(output.getName(), out);
            sec.addLineageDataSet(output.getName(), input1.getName());
        }

        /**
         *
         * @param sec
         * @throws DMLRuntimeException
         */
        private void updateReorgMatrixCharacteristics(FlinkExecutionContext sec)
                throws DMLRuntimeException
        {
            MatrixCharacteristics mc1 = sec.getMatrixCharacteristics(input1.getName());
            MatrixCharacteristics mcOut = sec.getMatrixCharacteristics(output.getName());

            //infer initially unknown dimensions from inputs
            if( !mcOut.dimsKnown() )
            {
                if( !mc1.dimsKnown() )
                    throw new DMLRuntimeException("Unable to compute output matrix characteristics from input.");

                if ( getOpcode().equalsIgnoreCase("r'") )
                    mcOut.set(mc1.getCols(), mc1.getRows(), mc1.getColsPerBlock(), mc1.getRowsPerBlock());
                else if ( getOpcode().equalsIgnoreCase("rdiag") )
                    mcOut.set(mc1.getRows(), (mc1.getCols()>1)?1:mc1.getRows(), mc1.getRowsPerBlock(), mc1.getColsPerBlock());
                else if ( getOpcode().equalsIgnoreCase("rsort") ) {
                    boolean ixret = sec.getScalarInput(_ixret.getName(), _ixret.getValueType(), _ixret.isLiteral()).getBooleanValue();
                    mcOut.set(mc1.getRows(), ixret?1:mc1.getCols(), mc1.getRowsPerBlock(), mc1.getColsPerBlock());
                }
            }

            //infer initially unknown nnz from input
            if( !mcOut.nnzKnown() && mc1.nnzKnown() ){
                boolean sortIx = getOpcode().equalsIgnoreCase("rsort") && sec.getScalarInput(_ixret.getName(), _ixret.getValueType(), _ixret.isLiteral()).getBooleanValue();
                if( sortIx )
                    mcOut.setNonZeros(mc1.getRows());
                else //default (r', rdiag, rsort data)
                    mcOut.setNonZeros(mc1.getNonZeros());
            }
        }
    }


