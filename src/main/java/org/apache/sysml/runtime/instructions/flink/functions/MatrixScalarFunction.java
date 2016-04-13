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
import org.apache.sysml.runtime.matrix.operators.ScalarOperator;

/**
 * Flink map function that applies a scalar operation to a blocks.
 */
public class MatrixScalarFunction
        implements MapFunction<Tuple2<MatrixIndexes, MatrixBlock>, Tuple2<MatrixIndexes, MatrixBlock>> {

    private final ScalarOperator operator;
    private final Tuple2<MatrixIndexes, MatrixBlock> output = new Tuple2<MatrixIndexes, MatrixBlock>();

    public MatrixScalarFunction(ScalarOperator operator) {
        this.operator = operator;
    }

    @Override
    public Tuple2<MatrixIndexes, MatrixBlock> map(Tuple2<MatrixIndexes, MatrixBlock> value) throws Exception {
        output.f0 = value.f0;
        output.f1 = (MatrixBlock) value.f1.scalarOperations(operator, new MatrixBlock());
        return output;
    }
}
