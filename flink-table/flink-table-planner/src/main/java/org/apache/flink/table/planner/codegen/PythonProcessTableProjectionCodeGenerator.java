/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.codegen;

import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.RexTableArgCall;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/** Generates the single-row argument projection consumed by a Python PTF worker. */
public final class PythonProcessTableProjectionCodeGenerator {

    private PythonProcessTableProjectionCodeGenerator() {}

    public static Result generate(
            CodeGeneratorContext ctx,
            RexCall udfCall,
            RowType inputType,
            List<String> argumentNames) {
        final ExprCodeGenerator expressionGenerator =
                new ExprCodeGenerator(ctx, false)
                        .bindInput(
                                inputType,
                                CodeGenUtils.DEFAULT_INPUT1_TERM(),
                                JavaScalaConversionUtil.toScala(Optional.empty()));
        final List<GeneratedExpression> expressions = new ArrayList<>();
        for (RexNode operand : udfCall.getOperands()) {
            if (operand instanceof RexTableArgCall) {
                expressions.add(
                        new GeneratedExpression(
                                CodeGenUtils.DEFAULT_INPUT1_TERM(),
                                "false",
                                "",
                                FlinkTypeFactory.toLogicalType(operand.getType()),
                                JavaScalaConversionUtil.toScala(Optional.empty())));
            } else {
                expressions.add(expressionGenerator.generateExpression(operand));
            }
        }
        final LogicalType[] fieldTypes =
                expressions.stream()
                        .map(GeneratedExpression::resultType)
                        .toArray(LogicalType[]::new);
        final RowType argumentType = RowType.of(fieldTypes, argumentNames.toArray(new String[0]));
        final int[] inputMapping = new int[fieldTypes.length];
        Arrays.fill(inputMapping, ProjectionCodeGenerator.EMPTY_INPUT_MAPPING_VALUE());
        final GeneratedProjection projection =
                ProjectionCodeGenerator.generateProjection(
                        ctx,
                        "PythonProcessTableArgumentProjection",
                        inputType,
                        argumentType,
                        inputMapping,
                        BinaryRowData.class,
                        CodeGenUtils.DEFAULT_INPUT1_TERM(),
                        CodeGenUtils.DEFAULT_OUT_RECORD_TERM(),
                        CodeGenUtils.DEFAULT_OUT_RECORD_WRITER_TERM(),
                        true,
                        expressions.toArray(new GeneratedExpression[0]));
        return new Result(projection, argumentType);
    }

    /** Projection and its output type. */
    public static final class Result {
        private final GeneratedProjection projection;
        private final RowType argumentType;

        private Result(GeneratedProjection projection, RowType argumentType) {
            this.projection = projection;
            this.argumentType = argumentType;
        }

        public GeneratedProjection getProjection() {
            return projection;
        }

        public RowType getArgumentType() {
            return argumentType;
        }
    }
}
