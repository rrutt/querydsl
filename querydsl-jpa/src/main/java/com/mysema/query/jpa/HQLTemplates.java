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
package com.mysema.query.jpa;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.mysema.query.types.Operator;
import com.mysema.query.types.Ops;
import com.mysema.query.types.PathType;

/**
 * HQLTemplates extends JPQLTemplates with Hibernate specific extensions
 *
 * @author tiwe
 *
 */
public class HQLTemplates extends JPQLTemplates {

    private static final QueryHandler QUERY_HANDLER;

    static {
        QueryHandler instance;
        try {
            instance = (QueryHandler) Class.forName("com.mysema.query.jpa.HibernateHandler").newInstance();
        } catch (NoClassDefFoundError e) {
            instance = DefaultQueryHandler.DEFAULT;
        } catch (Exception e) {
            instance = DefaultQueryHandler.DEFAULT;
        }
        QUERY_HANDLER = instance;
    }

    private static final List<Operator<?>> wrapElements = Arrays.<Operator<?>> asList(
            Ops.QuantOps.ALL,
            Ops.QuantOps.ANY,
            Ops.QuantOps.AVG_IN_COL,
            Ops.EXISTS);

    public static final HQLTemplates DEFAULT = new HQLTemplates();

    public HQLTemplates() {
        this(DEFAULT_ESCAPE);
    }

    public HQLTemplates(char escape) {
        super(escape, QUERY_HANDLER);
     // TODO : remove this when Hibernate supports type(alias)
        add(Ops.INSTANCE_OF, "{0}.class = {1}");
     // TODO : remove this when Hibernate supports type(alias)
        add(TYPE, "{0}.class");
     // TODO : remove this when Hibernate supports member of properly
        add(MEMBER_OF, "{0} in elements({1})");
        add(NOT_MEMBER_OF, "{0} not in elements({1})");

        // path types
        for (PathType type : new PathType[] {
                PathType.LISTVALUE,
                PathType.MAPVALUE,
                PathType.MAPVALUE_CONSTANT }) {
            add(type, "{0}[{1}]");
        }
        add(PathType.LISTVALUE_CONSTANT, "{0}[{1s}]");
        add(PathType.COLLECTION_ANY, "any elements({0})");

        add(Ops.CONTAINS_KEY, "{1} in indices({0})");
        add(Ops.CONTAINS_VALUE, "{1} in elements({0})");
    }

    @Override
    public boolean wrapElements(Operator<?> operator) {
        return wrapElements.contains(operator);
    }

    @Override
    public boolean isTypeAsString() {
        return true;
    }

    @Override
    public String getExistsProjection() {
        return "1";
    }

    @Override
    public boolean isSelect1Supported() {
        // TODO return true, when JPQLTemplates becomes standard
        return false;
    }

    @Override
    public boolean isEnumInPathSupported() {
        // related : http://opensource.atlassian.com/projects/hibernate/browse/HHH-5159
        return false;
    }

    @Override
    public boolean wrapConstant(Object constant) {
        // related : https://hibernate.onjira.com/browse/HHH-6913
        Class<?> type = constant.getClass();
        return type.isArray() || Collection.class.isAssignableFrom(type);
    }

    @Override
    public boolean isWithForOn() {
        return true;
    }

}
