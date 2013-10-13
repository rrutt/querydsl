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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mysema.query.DefaultQueryMetadata;
import com.mysema.query.JoinType;
import com.mysema.query.QueryException;
import com.mysema.query.QueryFlag;
import com.mysema.query.QueryFlag.Position;
import com.mysema.query.QueryMetadata;
import com.mysema.query.dml.DeleteClause;
import com.mysema.query.sql.Configuration;
import com.mysema.query.sql.RelationalPath;
import com.mysema.query.sql.SQLSerializer;
import com.mysema.query.sql.SQLTemplates;
import com.mysema.query.types.Expression;
import com.mysema.query.types.Predicate;

/**
 * SQLDeleteClause defines a DELETE clause
 *
 * @author tiwe
 *
 */
public class SQLDeleteClause extends AbstractSQLClause<SQLDeleteClause> implements DeleteClause<SQLDeleteClause> {

    private static final Logger logger = LoggerFactory.getLogger(SQLDeleteClause.class);

    private final Connection connection;

    private final RelationalPath<?> entity;

    private final List<QueryMetadata> batches = new ArrayList<QueryMetadata>();

    private QueryMetadata metadata = new DefaultQueryMetadata();

    private transient String queryString;

    public SQLDeleteClause(Connection connection, SQLTemplates templates, RelationalPath<?> entity) {
        this(connection, new Configuration(templates), entity);
    }

    public SQLDeleteClause(Connection connection, Configuration configuration, RelationalPath<?> entity) {
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
    public SQLDeleteClause addFlag(Position position, String flag) {
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
    public SQLDeleteClause addFlag(Position position, Expression<?> flag) {
        metadata.addFlag(new QueryFlag(position, flag));
        return this;
    }

    /**
     * Add current state of bindings as a batch item
     *
     * @return
     */
    public SQLDeleteClause addBatch() {
        batches.add(metadata);
        metadata = new DefaultQueryMetadata();
        metadata.addJoin(JoinType.DEFAULT, entity);
        return this;
    }

    private PreparedStatement createStatement() throws SQLException{
        PreparedStatement stmt;
        if (batches.isEmpty()) {
            SQLSerializer serializer = new SQLSerializer(configuration, true);
            serializer.serializeForDelete(metadata, entity);
            queryString = serializer.toString();
            logger.debug(queryString);
            stmt = connection.prepareStatement(queryString);
            setParameters(stmt, serializer.getConstants(), serializer.getConstantPaths(), metadata.getParams());
        } else {
            SQLSerializer serializer = new SQLSerializer(configuration, true);
            serializer.serializeForDelete(batches.get(0), entity);
            queryString = serializer.toString();
            logger.debug(queryString);

            // add first batch
            stmt = connection.prepareStatement(queryString);
            setParameters(stmt, serializer.getConstants(), serializer.getConstantPaths(), metadata.getParams());
            stmt.addBatch();

            // add other batches
            for (int i = 1; i < batches.size(); i++) {
                serializer = new SQLSerializer(configuration, true);
                serializer.serializeForDelete(batches.get(i), entity);
                setParameters(stmt, serializer.getConstants(), serializer.getConstantPaths(), metadata.getParams());
                stmt.addBatch();
            }
        }
        return stmt;
    }

    @Override
    public long execute() {
        PreparedStatement stmt = null;
        try {
            stmt = createStatement();
            if (batches.isEmpty()) {
                listeners.notifyDelete(metadata, entity);
                return stmt.executeUpdate();
            } else {
                listeners.notifyDeletes(metadata, entity, batches);
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

    public SQLDeleteClause where(Predicate p) {
        metadata.addWhere(p);
        return this;
    }

    @Override
    public SQLDeleteClause where(Predicate... o) {
        for (Predicate p : o) {
            metadata.addWhere(p);
        }
        return this;
    }

    @Override
    public String toString() {
        SQLSerializer serializer = new SQLSerializer(configuration, true);
        serializer.serializeForDelete(metadata, entity);
        return serializer.toString();
    }

}
