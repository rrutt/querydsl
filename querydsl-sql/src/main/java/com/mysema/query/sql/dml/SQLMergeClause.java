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
package com.mysema.query.sql.dml;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mysema.query.DefaultQueryMetadata;
import com.mysema.query.JoinType;
import com.mysema.query.QueryException;
import com.mysema.query.QueryFlag;
import com.mysema.query.QueryFlag.Position;
import com.mysema.query.QueryMetadata;
import com.mysema.query.dml.StoreClause;
import com.mysema.query.sql.Configuration;
import com.mysema.query.sql.RelationalPath;
import com.mysema.query.sql.SQLQuery;
import com.mysema.query.sql.SQLSerializer;
import com.mysema.query.sql.SQLTemplates;
import com.mysema.query.sql.types.Null;
import com.mysema.query.types.ConstantImpl;
import com.mysema.query.types.Expression;
import com.mysema.query.types.ExpressionUtils;
import com.mysema.query.types.NullExpression;
import com.mysema.query.types.Path;
import com.mysema.query.types.SubQueryExpression;
import com.mysema.util.ResultSetAdapter;

/**
 * SQLMergeClause defines an MERGE INTO clause
 *
 * @author tiwe
 *
 */
public class SQLMergeClause extends AbstractSQLClause<SQLMergeClause> implements StoreClause<SQLMergeClause> {

    private static final Logger logger = LoggerFactory.getLogger(SQLMergeClause.class);

    private final List<Path<?>> columns = new ArrayList<Path<?>>();

    private final Connection connection;

    private final RelationalPath<?> entity;

    private final QueryMetadata metadata = new DefaultQueryMetadata();

    private final List<Path<?>> keys = new ArrayList<Path<?>>();

    @Nullable
    private SubQueryExpression<?> subQuery;

    private final List<SQLMergeBatch> batches = new ArrayList<SQLMergeBatch>();

    private final List<Expression<?>> values = new ArrayList<Expression<?>>();

    private transient String queryString;

    public SQLMergeClause(Connection connection, SQLTemplates templates, RelationalPath<?> entity) {
        this(connection, new Configuration(templates), entity);
    }

    public SQLMergeClause(Connection connection, Configuration configuration, RelationalPath<?> entity) {
        super(configuration);
        this.connection = connection;
        this.entity = entity;
        metadata.addJoin(JoinType.DEFAULT, entity);
    }

    /**
     * Add the given String literal at the given position as a query flag
     *
     * @param position
     * @param flag
     * @return
     */
    public SQLMergeClause addFlag(Position position, String flag) {
        metadata.addFlag(new QueryFlag(position, flag));
        return this;
    }

    /**
     * Add the given Expression at the given position as a query flag
     *
     * @param position
     * @param flag
     * @return
     */
    public SQLMergeClause addFlag(Position position, Expression<?> flag) {
        metadata.addFlag(new QueryFlag(position, flag));
        return this;
    }

    /**
     * Add the current state of bindings as a batch item
     *
     * @return
     */
    public SQLMergeClause addBatch() {
        if (!configuration.getTemplates().isNativeMerge()) {
            throw new IllegalStateException("batch only supported for databases that support native merge");
        }

        batches.add(new SQLMergeBatch(keys, columns, values, subQuery));
        columns.clear();
        values.clear();
        keys.clear();
        subQuery = null;
        return this;
    }

    public SQLMergeClause columns(Path<?>... columns) {
        this.columns.addAll(Arrays.asList(columns));
        return this;
    }

    /**
     * Execute the clause and return the generated key with the type of the given path.
     * If no rows were created, null is returned, otherwise the key of the first row is returned.
     *
     * @param <T>
     * @param path
     * @return
     */
    @SuppressWarnings("unchecked")
    @Nullable
    public <T> T executeWithKey(Path<T> path) {
        return executeWithKey((Class<T>)path.getType(), path);
    }

    /**
     * Execute the clause and return the generated key cast to the given type.
     * If no rows were created, null is returned, otherwise the key of the first row is returned.
     *
     * @param <T>
     * @param type
     * @return
     */
    public <T> T executeWithKey(Class<T> type) {
        return executeWithKey(type, null);
    }

    private <T> T executeWithKey(Class<T> type, @Nullable Path<T> path) {
        ResultSet rs = executeWithKeys();
        try{
            if (rs.next()) {
                return configuration.get(rs, path, 1, type);
            } else {
                return null;
            }
        } catch (SQLException e) {
            throw new QueryException(e.getMessage(), e);
        }finally{
            close(rs);
        }
    }

    /**
     * Execute the clause and return the generated key with the type of the given path.
     * If no rows were created, or the referenced column is not a generated key, null is returned.
     * Otherwise, the key of the first row is returned.
     *
     * @param <T>
     * @param path
     * @return
     */
    @SuppressWarnings("unchecked")
    public <T> List<T> executeWithKeys(Path<T> path) {
        return executeWithKeys((Class<T>)path.getType(), path);
    }

    public <T> List<T> executeWithKeys(Class<T> type) {
        return executeWithKeys(type, null);
    }

    private <T> List<T> executeWithKeys(Class<T> type, @Nullable Path<T> path) {
        ResultSet rs = executeWithKeys();
        try{
            List<T> rv = new ArrayList<T>();
            while (rs.next()) {
                rv.add(configuration.get(rs, path, 1, type));
            }
            return rv;
        } catch (SQLException e) {
            throw new QueryException(e.getMessage(), e);
        }finally{
            close(rs);
        }
    }

    /**
     * Execute the clause and return the generated keys as a ResultSet
     *
     * @return
     */
    public ResultSet executeWithKeys() {
        try {
            if (configuration.getTemplates().isNativeMerge()) {
                final PreparedStatement stmt = createStatement(true);
                if (batches.isEmpty()) {
                    listeners.notifyMerge(metadata, entity, keys, columns, values, subQuery);
                    stmt.executeUpdate();
                } else {
                    listeners.notifyMerges(metadata, entity, batches);
                    stmt.executeBatch();
                }
                ResultSet rs = stmt.getGeneratedKeys();
                return new ResultSetAdapter(rs) {
                    @Override
                    public void close() throws SQLException {
                        try {
                            super.close();
                        } finally {
                            stmt.close();
                        }
                    }
                };
            } else {
                List<?> ids = getIds();
                if (!ids.isEmpty()) {
                    // update
                    SQLUpdateClause update = new SQLUpdateClause(connection, configuration.getTemplates(), entity);
                    populate(update);
                    update.where(ExpressionUtils.in((Expression)keys.get(0),ids));
                    return EmptyResultSet.DEFAULT;
                } else {
                    // insert
                    SQLInsertClause insert = new SQLInsertClause(connection, configuration.getTemplates(), entity);
                    populate(insert);
                    return insert.executeWithKeys();
                }
            }
        } catch (SQLException e) {
            throw new QueryException("Caught " + e.getClass().getSimpleName() + " for " + queryString, e);
        }
    }

    @Override
    public long execute() {
        if (configuration.getTemplates().isNativeMerge()) {
            return executeNativeMerge();
        } else {
            return executeCompositeMerge();
        }
    }

    private List<?> getIds() {
         // select
        SQLQuery query = new SQLQuery(connection, configuration.getTemplates()).from(entity);
        for (int i=0; i < columns.size(); i++) {
            if (values.get(i) instanceof NullExpression) {
                query.where(ExpressionUtils.isNull(columns.get(i)));
            } else {
                query.where(ExpressionUtils.eq(columns.get(i),(Expression)values.get(i)));
            }
        }
        List<?> ids = query.list(keys.get(0));
        return ids;
    }

    @SuppressWarnings("unchecked")
    private long executeCompositeMerge() {
        List<?> ids = getIds();
        if (!ids.isEmpty()) {
            // update
            SQLUpdateClause update = new SQLUpdateClause(connection, configuration.getTemplates(), entity);
            populate(update);
            update.where(ExpressionUtils.in((Expression)keys.get(0),ids));
            return update.execute();
        } else {
            // insert
            SQLInsertClause insert = new SQLInsertClause(connection, configuration.getTemplates(), entity);
            populate(insert);
            return insert.execute();

        }
    }

    @SuppressWarnings("unchecked")
    private void populate(StoreClause<?> clause) {
        for (int i = 0; i < columns.size(); i++) {
            clause.set((Path)columns.get(i), (Object)values.get(i));
        }
    }

    private PreparedStatement createStatement(boolean withKeys) throws SQLException{
        SQLSerializer serializer = new SQLSerializer(configuration, true);
        PreparedStatement stmt = null;
        if (batches.isEmpty()) {
            serializer.serializeForMerge(metadata, entity, keys, columns, values, subQuery);
            stmt = prepareStatementAndSetParameters(serializer, withKeys);
        } else {
            serializer.serializeForMerge(metadata, entity,
                    batches.get(0).getKeys(), batches.get(0).getColumns(),
                    batches.get(0).getValues(), batches.get(0).getSubQuery());
            stmt = prepareStatementAndSetParameters(serializer, withKeys);

            // add first batch
            stmt.addBatch();

            // add other batches
            for (int i = 1; i < batches.size(); i++) {
                SQLMergeBatch batch = batches.get(i);
                serializer = new SQLSerializer(configuration, true);
                serializer.serializeForMerge(metadata, entity, batch.getKeys(), batch.getColumns(), batch.getValues(), batch.getSubQuery());
                setParameters(stmt, serializer.getConstants(), serializer.getConstantPaths(), metadata.getParams());
                stmt.addBatch();
            }
        }
        return stmt;
    }

    private PreparedStatement prepareStatementAndSetParameters(SQLSerializer serializer,
            boolean withKeys) throws SQLException {
        queryString = serializer.toString();
        logger.debug(queryString);
        PreparedStatement stmt;
        if (withKeys) {
            String[] target = new String[keys.size()];
            for (int i = 0; i < target.length; i++) {
                target[i] = keys.get(i).getMetadata().getName();
            }
            stmt = connection.prepareStatement(queryString, target);
        } else {
            stmt = connection.prepareStatement(queryString);
        }
        setParameters(stmt, serializer.getConstants(), serializer.getConstantPaths(), metadata.getParams());
        return stmt;
    }

    private long executeNativeMerge() {
        PreparedStatement stmt = null;
        try {
            stmt = createStatement(false);
            if (batches.isEmpty()) {
                listeners.notifyMerge(metadata, entity, keys, columns, values, subQuery);
                return stmt.executeUpdate();
            } else {
                listeners.notifyMerges(metadata, entity, batches);
                return executeBatch(stmt);
            }
        } catch (SQLException e) {
            throw new QueryException("Caught " + e.getClass().getSimpleName() + " for " + queryString, e);
        } finally {
            if (stmt != null) {
                close(stmt);
            }
        }
    }

    /**
     * Set the keys to be used in the MERGE clause
     *
     * @param paths
     * @return
     */
    public SQLMergeClause keys(Path<?>... paths) {
        for (Path<?> path : paths) {
            keys.add(path);
        }
        return this;
    }

    public SQLMergeClause select(SubQueryExpression<?> subQuery) {
        this.subQuery = subQuery;
        return this;
    }

    @Override
    public <T> SQLMergeClause set(Path<T> path, @Nullable T value) {
        columns.add(path);
        if (value != null) {
            values.add(new ConstantImpl<T>(value));
        } else {
            values.add(Null.CONSTANT);
        }
        return this;
    }

    @Override
    public <T> SQLMergeClause set(Path<T> path, Expression<? extends T> expression) {
        columns.add(path);
        values.add(expression);
        return this;
    }

    @Override
    public <T> SQLMergeClause setNull(Path<T> path) {
        columns.add(path);
        values.add(Null.CONSTANT);
        return this;
    }

    @Override
    public String toString() {
        SQLSerializer serializer = new SQLSerializer(configuration, true);
        serializer.serializeForMerge(metadata, entity, keys, columns, values, subQuery);
        return serializer.toString();
    }

    public SQLMergeClause values(Object... v) {
        for (Object value : v) {
            if (value instanceof Expression<?>) {
                values.add((Expression<?>) value);
            } else if (value != null) {
                values.add(new ConstantImpl<Object>(value));
            } else {
                values.add(Null.CONSTANT);
            }
        }
        return this;
    }

    @Override
    public boolean isEmpty() {
        return values.isEmpty();
    }

}
