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

package org.apache.sysml.runtime.instructions.flink.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.operators.AggregateUnaryOperator;

public class UAggValueFunction implements MapFunction<Tuple2<MatrixIndexes, MatrixBlock>,
        Tuple2<MatrixIndexes, MatrixBlock>> {
    private static final long serialVersionUID = 5352374590399929673L;

    private AggregateUnaryOperator _op = null;
    private int _brlen = -1;
    private int _bclen = -1;
    private MatrixIndexes _ix = null;

    public UAggValueFunction(AggregateUnaryOperator op, int brlen, int bclen) {
        _op = op;
        _brlen = brlen;
        _bclen = bclen;

        _ix = new MatrixIndexes(1, 1);
    }

    @Override
    public Tuple2<MatrixIndexes, MatrixBlock> map(Tuple2<MatrixIndexes, MatrixBlock> value) throws Exception {
        MatrixBlock blkOut = new MatrixBlock();

        //unary aggregate operation
        value.f1.aggregateUnaryOperations(_op, blkOut, _brlen, _bclen, _ix);

        //always drop correction since no aggregation
        blkOut.dropLastRowsOrColums(_op.aggOp.correctionLocation);

        value.f1 = blkOut;
        //output new tuple
        return value;
    }
}
