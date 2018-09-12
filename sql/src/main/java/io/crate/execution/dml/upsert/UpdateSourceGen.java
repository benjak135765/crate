/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.dml.upsert;

import io.crate.core.collections.StringObjectMaps;
import io.crate.data.Input;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.expression.BaseImplementationSymbolVisitor;
import io.crate.expression.InputFactory;
import io.crate.expression.ValueExtractors;
import io.crate.expression.reference.GetResultRefResolver;
import io.crate.expression.reference.ReferenceResolver;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocTableInfo;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.get.GetResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Used to apply update expressions to create a updated source
 *
 * <pre>
 * {@code
 * For updates:
 *
 *  UPDATE t SET x = x + 10
 *
 *      getResult:  {x: 20, y: 30}
 *      updateAssignments: x + 10
 *          (x = Reference)
 *          (10 = Literal)
 *      insertValues: null
 *
 *      resultSource: {x: 30, y: 30}
 *
 *
 * For ON CONFLICT DO UPDATE:
 *
 *  INSERT INTO t VALUES (10) ON CONFLICT .. DO UPDATE SET x = x + excluded.x
 *      getResult: {x: 20, y: 30}
 *      updateAssignments: x + excluded.x
 *          (x = Reference)
 *          (excluded.x = Reference)
 *      insertValues: [10]
 *      
 *      resultSource: {x: 30, y: 30}
 * </pre>
 */
final class UpdateSourceGen {

    private final Evaluator eval;
    private final GeneratedColumns<GetResult> generatedColumns;
    private final ArrayList<Reference> updateColumns;
    private final CheckConstraints<GetResult, CollectExpression<GetResult, ?>> checks;

    UpdateSourceGen(Functions functions, DocTableInfo table, String[] updateColumns) {
        GetResultRefResolver refResolver = new GetResultRefResolver(table.partitionedBy());
        this.eval = new Evaluator(functions, refResolver);
        InputFactory inputFactory = new InputFactory(functions);
        this.checks = new CheckConstraints<>(inputFactory, refResolver, table);
        this.updateColumns = new ArrayList<>(updateColumns.length);
        for (String updateColumn : updateColumns) {
            ColumnIdent column = ColumnIdent.fromPath(updateColumn);
            Reference ref = table.getReference(column);
            this.updateColumns.add(ref == null ? table.getDynamic(column, true) : ref);
        }
        if (table.generatedColumns().isEmpty()) {
            generatedColumns = GeneratedColumns.empty();
        } else {
            generatedColumns = new GeneratedColumns<>(
                inputFactory,
                InsertSourceGen.Validation.GENERATED_VALUE_MATCH,
                refResolver,
                this.updateColumns,
                table.generatedColumns()
            );
        }
    }

    BytesReference generateSource(GetResult result, Symbol[] updateAssignments, Object[] insertValues) throws IOException {
        /* We require a new HashMap because all evaluations of the updateAssignments need to be based on the
         * values *before* the update. For example:
         *
         * source: x=5
         * SET x = 10, y = x + 5
         *
         * Must result in y = 10, not 15
         */
        Values values = new Values(result, insertValues);
        HashMap<String, Object> updatedSource = new HashMap<>(result.getSource());
        for (int i = 0; i < updateColumns.size(); i++) {
            Reference ref = updateColumns.get(i);
            Object value = eval.process(updateAssignments[i], values).value();
            ColumnIdent column = ref.column();
            StringObjectMaps.mergeInto(updatedSource, column.name(), column.path(), value);
        }
        injectGeneratedColumns(result, updatedSource);
        BytesReference updatedSourceRef = XContentFactory.jsonBuilder().map(updatedSource).bytes();
        if (checks.hasChecks() || generatedColumns.hasColumnsToValidate()) {
            GetResult updatedGetResult = new GetResult(
                result.getIndex(),
                result.getType(),
                result.getId(),
                result.getVersion(),
                result.isExists(),
                updatedSourceRef,
                result.getFields()
            );
            if (generatedColumns.hasColumnsToValidate()) {
                generatedColumns.setNextRow(updatedGetResult);
                for (int i = 0; i < updateColumns.size(); i++) {
                    Reference ref = updateColumns.get(i);
                    ColumnIdent column = ref.column();
                    Object val = ref.valueType().value(ValueExtractors.fromMap(updatedSource, column));
                    generatedColumns.validateValue(ref, val);
                }
            }
            checks.validate(updatedGetResult);
        }
        return updatedSourceRef;
    }

    private void injectGeneratedColumns(GetResult result, HashMap<String, Object> updatedSource) throws IOException {
        if (!generatedColumns.hasColumnsToInject()) {
            return;
        }
        BytesReference updatedSourceRef = XContentFactory.jsonBuilder().map(updatedSource).bytes();
        GetResult updatedGetResult = new GetResult(
            result.getIndex(),
            result.getType(),
            result.getId(),
            result.getVersion(),
            result.isExists(),
            updatedSourceRef,
            result.getFields()
        );
        generatedColumns.setNextRow(updatedGetResult);
        for (Map.Entry<Reference, Input<?>> entry : generatedColumns.toInject()) {
            ColumnIdent column = entry.getKey().column();
            Object value = entry.getValue().value();
            StringObjectMaps.mergeInto(updatedSource, column.name(), column.path(), value);
        }
    }

    private static class Values {

        private final GetResult getResult;
        private final Object[] insertValues;

        Values(GetResult getResult, Object[] insertValues) {
            this.getResult = getResult;
            this.insertValues = insertValues;
        }
    }

    private static class Evaluator extends BaseImplementationSymbolVisitor<Values> {

        private final ReferenceResolver<CollectExpression<GetResult, ?>> refResolver;

        private Evaluator(Functions functions,
                          ReferenceResolver<CollectExpression<GetResult, ?>> refResolver) {
            super(functions);
            this.refResolver = refResolver;
        }

        @Override
        public Input<?> visitInputColumn(InputColumn inputColumn, Values context) {
            return Literal.of(inputColumn.valueType(), context.insertValues[inputColumn.index()]);
        }

        @Override
        public Input<?> visitReference(Reference symbol, Values values) {
            CollectExpression<GetResult, ?> expr = refResolver.getImplementation(symbol);
            expr.setNextRow(values.getResult);
            return expr;
        }
    }
}
