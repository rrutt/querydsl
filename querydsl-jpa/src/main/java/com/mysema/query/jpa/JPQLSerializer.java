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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;
import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;
import javax.persistence.EntityManager;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.PersistenceUnitUtil;
import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.Metamodel;
import javax.persistence.metamodel.SingularAttribute;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.mysema.query.JoinExpression;
import com.mysema.query.JoinType;
import com.mysema.query.QueryMetadata;
import com.mysema.query.support.SerializerBase;
import com.mysema.query.types.Constant;
import com.mysema.query.types.ConstantImpl;
import com.mysema.query.types.EntityPath;
import com.mysema.query.types.Expression;
import com.mysema.query.types.ExpressionUtils;
import com.mysema.query.types.FactoryExpression;
import com.mysema.query.types.Operation;
import com.mysema.query.types.Operator;
import com.mysema.query.types.Ops;
import com.mysema.query.types.Order;
import com.mysema.query.types.OrderSpecifier;
import com.mysema.query.types.ParamExpression;
import com.mysema.query.types.Path;
import com.mysema.query.types.PathImpl;
import com.mysema.query.types.PathType;
import com.mysema.query.types.Predicate;
import com.mysema.query.types.SubQueryExpression;
import com.mysema.util.MathUtils;

/**
 * JPQLSerializer serializes Querydsl expressions into JPQL syntax.
 *
 * @author tiwe
 */
public class JPQLSerializer extends SerializerBase<JPQLSerializer> {

    private static final Set<Operator<?>> NUMERIC = ImmutableSet.<Operator<?>>of(
            Ops.ADD, Ops.SUB, Ops.MULT, Ops.DIV,
            Ops.LT, Ops.LOE, Ops.GT, Ops.GOE, Ops.BETWEEN);

    private static final String COMMA = ", ";

    private static final String DELETE = "delete from ";

    private static final String FROM = "from ";

    private static final String GROUP_BY = "\ngroup by ";

    private static final String HAVING = "\nhaving ";

    private static final String ORDER_BY = "\norder by ";

    private static final String SELECT = "select ";

    private static final String SELECT_COUNT = "select count(";

    private static final String SELECT_COUNT_DISTINCT = "select count(distinct ";

    private static final String SELECT_DISTINCT = "select distinct ";

    private static final String SET = "\nset ";

    private static final String UPDATE = "update ";

    private static final String WHERE = "\nwhere ";

    private static final String WITH = " with ";

    private static final String ON = " on ";

    private static final Map<JoinType, String> joinTypes = new HashMap<JoinType, String>();

    private final JPQLTemplates templates;

    private final EntityManager entityManager;

    private boolean inProjection = false;

    static{
        joinTypes.put(JoinType.DEFAULT, COMMA);
        joinTypes.put(JoinType.FULLJOIN, "\n  full join ");
        joinTypes.put(JoinType.INNERJOIN, "\n  inner join ");
        joinTypes.put(JoinType.JOIN, "\n  inner join ");
        joinTypes.put(JoinType.LEFTJOIN, "\n  left join ");
        joinTypes.put(JoinType.RIGHTJOIN, "\n  right join ");
    }

    private boolean wrapElements = false;

    public JPQLSerializer(JPQLTemplates templates) {
        this(templates, null);
    }

    public JPQLSerializer(JPQLTemplates templates, EntityManager em) {
        super(templates);
        this.templates = templates;
        this.entityManager = em;
    }

    private void handleJoinTarget(JoinExpression je) {
        // type specifier
        if (je.getTarget() instanceof EntityPath<?>) {
            final EntityPath<?> pe = (EntityPath<?>) je.getTarget();
            if (pe.getMetadata().isRoot()) {
                final Entity entityAnnotation = pe.getAnnotatedElement().getAnnotation(Entity.class);
                if (entityAnnotation != null && entityAnnotation.name().length() > 0) {
                    append(entityAnnotation.name());
                } else if (pe.getType().getPackage() != null) {
                    final String pn = pe.getType().getPackage().getName();
                    final String typeName = pe.getType().getName().substring(pn.length() + 1);
                    append(typeName);
                } else {
                    append(pe.getType().getName());
                }
                append(" ");
            }
        }
        handle(je.getTarget());
    }

    public void serialize(QueryMetadata metadata, boolean forCountRow, @Nullable String projection) {
        final List<? extends Expression<?>> select = metadata.getProjection();
        final List<JoinExpression> joins = metadata.getJoins();
        final Predicate where = metadata.getWhere();
        final List<? extends Expression<?>> groupBy = metadata.getGroupBy();
        final Predicate having = metadata.getHaving();
        final List<OrderSpecifier<?>> orderBy = metadata.getOrderBy();

        // select
        boolean inProjectionOrig = inProjection;
        inProjection = true;
        if (projection != null) {
            append(SELECT).append(projection).append("\n");

        } else if (forCountRow) {
            if (!metadata.isDistinct()) {
                append(SELECT_COUNT);
            } else {
                append(SELECT_COUNT_DISTINCT);
            }
            if(!select.isEmpty()) {
                if (select.get(0) instanceof FactoryExpression) {
                    handle(joins.get(0).getTarget());
                } else {
                    // TODO : make sure this works
                    handle(COMMA, select);
                }
            } else {
                handle(joins.get(0).getTarget());
            }
            append(")\n");

        } else {
            if (!metadata.isDistinct()) {
                append(SELECT);
            } else {
                append(SELECT_DISTINCT);
            }
            if (!select.isEmpty()) {
                handle(COMMA, select);
            } else {
                handle(metadata.getJoins().get(0).getTarget());
            }
            append("\n");

        }
        inProjection = inProjectionOrig;

        // from
        append(FROM);
        serializeSources(forCountRow, joins);

        // where
        if (where != null) {
            append(WHERE).handle(where);
        }

        // group by
        if (!groupBy.isEmpty()) {
            append(GROUP_BY).handle(COMMA, groupBy);
        }

        // having
        if (having != null) {
            append(HAVING).handle(having);
        }

        // order by
        if (!orderBy.isEmpty() && !forCountRow) {
            append(ORDER_BY);
            boolean first = true;
            for (final OrderSpecifier<?> os : orderBy) {
                if (!first) {
                    append(COMMA);
                }
                handle(os.getTarget());
                append(os.getOrder() == Order.ASC ? " asc" : " desc");
                if (os.getNullHandling() == OrderSpecifier.NullHandling.NullsFirst) {
                    append(" nulls first");
                } else if (os.getNullHandling() == OrderSpecifier.NullHandling.NullsLast) {
                    append(" nulls last");
                }
                first = false;
            }
        }
    }

    public void serializeForDelete(QueryMetadata md) {
        append(DELETE);
        handleJoinTarget(md.getJoins().get(0));
        if (md.getWhere() != null) {
            append(WHERE).handle(md.getWhere());
        }
    }

    public void serializeForUpdate(QueryMetadata md) {
        append(UPDATE);
        handleJoinTarget(md.getJoins().get(0));
        append(SET);
        handle(COMMA, md.getProjection());
        if (md.getWhere() != null) {
            append(WHERE).handle(md.getWhere());
        }
    }

    private void serializeSources(boolean forCountRow, List<JoinExpression> joins) {
        for (int i = 0; i < joins.size(); i++) {
            final JoinExpression je = joins.get(i);
            if (i > 0) {
                append(joinTypes.get(je.getType()));
            }
            if (je.hasFlag(JPAQueryMixin.FETCH) && !forCountRow) {
                handle(JPAQueryMixin.FETCH);
            }
            handleJoinTarget(je);
            // XXX Hibernate specific flag
            if (je.hasFlag(JPAQueryMixin.FETCH_ALL_PROPERTIES) && !forCountRow) {
                handle(JPAQueryMixin.FETCH_ALL_PROPERTIES);
            }

            if (je.getCondition() != null) {
                append(templates.isWithForOn() ? WITH : ON);
                handle(je.getCondition());
            }
        }
    }

    @Override
    public void visitConstant(Object constant) {
        boolean wrap = templates.wrapConstant(constant);
        if (wrap) {
             append("(");
        }
        append("?");
        if (!getConstantToLabel().containsKey(constant)) {
            final String constLabel = String.valueOf(getConstantToLabel().size()+1);
            getConstantToLabel().put(constant, constLabel);
            append(constLabel);
        } else {
            append(getConstantToLabel().get(constant));
        }
        if (wrap) {
            append(")");
        }
    }

    @Override
    public Void visit(ParamExpression<?> param, Void context) {
        append("?");
        if (!getConstantToLabel().containsKey(param)) {
            final String paramLabel = String.valueOf(getConstantToLabel().size()+1);
            getConstantToLabel().put(param, paramLabel);
            append(paramLabel);
        } else {
            append(getConstantToLabel().get(param));
        }
        return null;
    }

    @Override
    public Void visit(SubQueryExpression<?> query, Void context) {
        append("(");
        serialize(query.getMetadata(), false, null);
        append(")");
        return null;
    }

    @Override
    public Void visit(Path<?> expr, Void context) {
        // only wrap a PathCollection, if it the pathType is PROPERTY
        boolean wrap = wrapElements
        && (Collection.class.isAssignableFrom(expr.getType()) || Map.class.isAssignableFrom(expr.getType()))
        && expr.getMetadata().getPathType().equals(PathType.PROPERTY);
        if (wrap) {
            append("elements(");
        }
        super.visit(expr, context);
        if (wrap) {
            append(")");
        }
        return null;
    }

    @Override
    public Void visit(FactoryExpression<?> expr, Void context) {
        if (!inProjection) {
            append("(");
            super.visit(expr, context);
            append(")");
        } else {
            super.visit(expr, context);
        }
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void visitOperation(Class<?> type, Operator<?> operator, List<? extends Expression<?>> args) {
        boolean old = wrapElements;
        wrapElements = templates.wrapElements(operator);

        if (operator == Ops.EQ && args.get(1) instanceof Operation &&
                ((Operation)args.get(1)).getOperator() == Ops.QuantOps.ANY) {
            args = ImmutableList.<Expression<?>>of(args.get(0), ((Operation)args.get(1)).getArg(0));
            visitOperation(type, Ops.IN, args);

        } else if (operator == Ops.NE && args.get(1) instanceof Operation &&
                ((Operation)args.get(1)).getOperator() == Ops.QuantOps.ANY) {
            args = ImmutableList.<Expression<?>>of(args.get(0), ((Operation)args.get(1)).getArg(0));
            visitOperation(type, Ops.NOT_IN, args);

        } else if (operator == Ops.IN || operator == Ops.NOT_IN) {
            if (args.get(1) instanceof Path) {
                visitAnyInPath(type, operator, args);
            } else if (args.get(0) instanceof Path && args.get(1) instanceof Constant) {
                visitPathInCollection(type, operator, args);
            } else {
                super.visitOperation(type, operator, args);
            }

        } else if (operator == Ops.INSTANCE_OF) {
            visitInstanceOf(type, operator, args);

        } else if (operator == Ops.NUMCAST) {
            visitNumCast(args);

        } else if (operator == Ops.EXISTS && args.get(0) instanceof SubQueryExpression) {
            final SubQueryExpression subQuery = (SubQueryExpression) args.get(0);
            append("exists (");
            serialize(subQuery.getMetadata(), false, templates.getExistsProjection());
            append(")");

        } else if (operator == Ops.MATCHES || operator == Ops.MATCHES_IC) {
            super.visitOperation(type, Ops.LIKE,
                    ImmutableList.of(args.get(0), ExpressionUtils.regexToLike((Expression<String>) args.get(1))));

        } else if (operator == Ops.LIKE && args.get(1) instanceof Constant) {
            final String escape = String.valueOf(templates.getEscapeChar());
            final String escaped = args.get(1).toString().replace(escape, escape + escape);
            super.visitOperation(String.class, Ops.LIKE,
                    ImmutableList.of(args.get(0), ConstantImpl.create(escaped)));

        } else if (NUMERIC.contains(operator)) {
            super.visitOperation(type, operator, normalizeNumericArgs(args));

        } else {
            super.visitOperation(type, operator, args);
        }

        wrapElements = old;
    }

    private void visitNumCast(List<? extends Expression<?>> args) {
        final Class<?> targetType = (Class<?>) ((Constant<?>) args.get(1)).getConstant();
        final String typeName = targetType.getSimpleName().toLowerCase(Locale.ENGLISH);
        visitOperation(targetType, JPQLTemplates.CAST, ImmutableList.of(args.get(0), ConstantImpl.create(typeName)));
    }

    private void visitInstanceOf(Class<?> type, Operator<?> operator,
            List<? extends Expression<?>> args) {
        if (templates.isTypeAsString()) {
            final List<Expression<?>> newArgs = new ArrayList<Expression<?>>(args);
            final Class<?> cl = ((Class<?>) ((Constant<?>) newArgs.get(1)).getConstant());
            // use discriminator value instead of fqnm
            if (cl.getAnnotation(DiscriminatorValue.class) != null) {
                newArgs.set(1, ConstantImpl.create(cl.getAnnotation(DiscriminatorValue.class).value()));
            } else {
                newArgs.set(1, ConstantImpl.create(cl.getSimpleName()));
            }
            super.visitOperation(type, operator, newArgs);
        } else {
            super.visitOperation(type, operator, args);
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void visitPathInCollection(Class<?> type, Operator<?> operator,
            List<? extends Expression<?>> args) {
        // NOTE turns entityPath in collection into entityPath.id in (collection of ids)
        if (entityManager != null && !templates.isPathInEntitiesSupported() && args.get(0).getType().isAnnotationPresent(Entity.class)) {
            Path<?> lhs = (Path<?>) args.get(0);
            Constant<?> rhs = (Constant<?>) args.get(1);
            final Metamodel metamodel = entityManager.getMetamodel();
            final PersistenceUnitUtil util = entityManager.getEntityManagerFactory().getPersistenceUnitUtil();
            final EntityType<?> entityType = metamodel.entity(args.get(0).getType());
            if (entityType.hasSingleIdAttribute()) {
                SingularAttribute<?,?> id = getIdProperty(entityType);
                // turn lhs into id path
                lhs = new PathImpl(id.getJavaType(), lhs, id.getName());
                // turn rhs into id collection
                Set ids = new HashSet();
                for (Object entity : (Collection<?>)rhs.getConstant()) {
                    ids.add(util.getIdentifier(entity));
                }
                rhs = new ConstantImpl(ids);
                args = ImmutableList.of(lhs, rhs);
            }
        }
        super.visitOperation(type, operator, args);
    }

    @SuppressWarnings("rawtypes")
    private SingularAttribute<?,?> getIdProperty(EntityType entity) {
        final Set<SingularAttribute> singularAttributes = entity.getSingularAttributes();
        for (final SingularAttribute singularAttribute : singularAttributes) {
            if (singularAttribute.isId()) {
                return singularAttribute;
            }
        }
        return null;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void visitAnyInPath(Class<?> type, Operator<?> operator, List<? extends Expression<?>> args) {
        if (!templates.isEnumInPathSupported() && args.get(0) instanceof Constant && Enum.class.isAssignableFrom(args.get(0).getType())) {
            final Enumerated enumerated = ((Path)args.get(1)).getAnnotatedElement().getAnnotation(Enumerated.class);
            final Enum constant = (Enum)((Constant)args.get(0)).getConstant();
            if (enumerated == null || enumerated.value() == EnumType.ORDINAL) {
                args = ImmutableList.of(ConstantImpl.create(constant.ordinal()), args.get(1));
            } else {
                args = ImmutableList.of(new ConstantImpl<String>(constant.name()), args.get(1));
            }
        }
        super.visitOperation(type,
                operator == Ops.IN ? JPQLTemplates.MEMBER_OF : JPQLTemplates.NOT_MEMBER_OF,
                args);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private List<? extends Expression<?>> normalizeNumericArgs(List<? extends Expression<?>> args) {
        boolean hasConstants = false;
        Class<? extends Number> numType = null;
        for (Expression<?> arg : args) {
            if (Number.class.isAssignableFrom(arg.getType())) {
                if (arg instanceof Constant) {
                    hasConstants = true;
                } else {
                    numType = (Class<? extends Number>) arg.getType();
                }
            }
        }
        if (hasConstants && numType != null) {
            final List<Expression<?>> newArgs = new ArrayList<Expression<?>>(args.size());
            for (final Expression<?> arg : args) {
                if (arg instanceof Constant && Number.class.isAssignableFrom(arg.getType())
                        && !arg.getType().equals(numType)) {
                    final Number number = (Number) ((Constant)arg).getConstant();
                    newArgs.add(new ConstantImpl(MathUtils.cast(number, (Class)numType)));
                } else {
                    newArgs.add(arg);
                }
            }
            return newArgs;
        } else {
            return args;
        }
    }

}
