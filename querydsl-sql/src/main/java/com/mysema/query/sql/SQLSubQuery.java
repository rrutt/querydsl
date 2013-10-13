/*
 * Copyright 2011, Mysema Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mysema.query.sql;

import java.util.Arrays;
import java.util.List;

import com.mysema.query.QueryMetadata;
import com.mysema.query.types.Expression;
import com.mysema.query.types.Operator;
import com.mysema.query.types.SubQueryExpression;
import com.mysema.query.types.expr.BooleanExpression;
import com.mysema.query.types.expr.DslExpression;
import com.mysema.query.types.expr.SimpleOperation;
import com.mysema.query.types.template.NumberTemplate;

/**
 * SQLSubQuery is a subquery implementation for SQL queries
 *
 * @author tiwe
 *
 */
public class SQLSubQuery extends AbstractSQLSubQuery<SQLSubQuery> {

    public SQLSubQuery() {
        super();
    }

    public SQLSubQuery(QueryMetadata metadata) {
        super(metadata);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private <T> DslExpression<T> union(Operator<Object> op, List<? extends SubQueryExpression<T>> sq) {
        Expression<T> rv = sq.get(0);
        for (int i = 1; i < sq.size(); i++) {
            rv = SimpleOperation.create((Class)rv.getType(), op, rv, sq.get(i));
        }
        return (DslExpression<T>)rv;
    }

    public <T> DslExpression<T> union(List<? extends SubQueryExpression<T>> sq) {
        return union(SQLTemplates.UNION, sq);
    }

    public <T> DslExpression<T> union(SubQueryExpression<T>... sq) {
        return union(Arrays.asList(sq));
    }

    public <T> DslExpression<T> unionAll(List<? extends SubQueryExpression<T>> sq) {
        return union(SQLTemplates.UNION_ALL, sq);
    }

    public <T> DslExpression<T> unionAll(SubQueryExpression<T>... sq) {
        return unionAll(Arrays.asList(sq));
    }

    @Override
    public BooleanExpression exists() {
        return unique(NumberTemplate.ONE).exists();
    }

    @Override
    public BooleanExpression notExists() {
        return exists().not();
    }

}
