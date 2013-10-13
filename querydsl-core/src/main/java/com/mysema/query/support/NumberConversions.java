/*
 * Copyright 2012, Mysema Ltd
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
package com.mysema.query.support;

import java.util.List;

import com.mysema.query.types.Expression;
import com.mysema.query.types.ExpressionBase;
import com.mysema.query.types.FactoryExpression;
import com.mysema.query.types.Visitor;
import com.mysema.util.MathUtils;

/**
 * NumberConversions ensures that the results of a projection involving numeric expressions
 * confirm to the types of the numeric expressions
 *
 * @author tiwe
 *
 * @param <T>
 */
public class NumberConversions<T> extends ExpressionBase<T> implements FactoryExpression<T> {

    private static final long serialVersionUID = -7834053123363933721L;

    private final FactoryExpression<T> expr;

    public NumberConversions(FactoryExpression<T> expr) {
        super(expr.getType());
        this.expr = expr;
    }

    @Override
    public <R, C> R accept(Visitor<R, C> v, C context) {
        return v.visit(this, context);
    }

    @Override
    public List<Expression<?>> getArgs() {
        return expr.getArgs();
    }

    @Override
    public T newInstance(Object... args) {
        for (int i = 0; i < args.length; i++) {
            Class<?> type = expr.getArgs().get(i).getType();
            if (args[i] instanceof Number && !args[i].getClass().equals(type)) {
                if (type.equals(Boolean.class)) {
                    args[i] = ((Number)args[i]).intValue() > 0;
                } else if (Number.class.isAssignableFrom(type)){
                    args[i] = MathUtils.cast((Number)args[i], (Class)type);
                }
            }
        }
        return expr.newInstance(args);
    }

}
