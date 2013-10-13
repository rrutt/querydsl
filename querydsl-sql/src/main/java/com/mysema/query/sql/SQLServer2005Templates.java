/*
 * Copyright 2013, Mysema Ltd
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

import com.mysema.query.QueryFlag;
import com.mysema.query.QueryFlag.Position;
import com.mysema.query.QueryMetadata;
import com.mysema.query.QueryModifiers;
import com.mysema.query.sql.mssql.RowNumber;
import com.mysema.query.sql.mssql.SQLServerGrammar;
import com.mysema.query.support.Expressions;
import com.mysema.query.types.OrderSpecifier;


/**
 * SQLServer2005Templates is an SQL dialect for Microsoft SQL Server 2005 and 2008
 *
 * @author tiwe
 */
public class SQLServer2005Templates extends SQLServerTemplates {

    public static Builder builder() {
        return new Builder() {
            @Override
            protected SQLTemplates build(char escape, boolean quote) {
                return new SQLServer2005Templates(escape, quote);
            }
        };
    }

    private String topTemplate = "top ({0}) ";

    private String limitOffsetTemplate = "row_number > {0} and row_number <= {1}";

    private String limitTemplate = "row_number <= {0}";

    private String offsetTemplate = "row_number > {0}";

    private String outerQueryStart = "with inner_query as \n(\n  ";

    private String outerQueryEnd = "\n)\nselect * \nfrom inner_query\nwhere ";

    public SQLServer2005Templates() {
        this('\\',false);
    }

    public SQLServer2005Templates(boolean quote) {
        this('\\',quote);
    }

    public SQLServer2005Templates(char escape, boolean quote) {
        super(escape, quote);
    }

    @Override
    public void serialize(QueryMetadata metadata, boolean forCountRow, SQLSerializer context) {
        if (!forCountRow && metadata.getModifiers().isRestricting() && !metadata.getJoins().isEmpty()) {
            QueryModifiers mod = metadata.getModifiers();
            if (mod.getOffset() == null) {
                // select top ...
                metadata = metadata.clone();
                metadata.addFlag(new QueryFlag(QueryFlag.Position.AFTER_SELECT,
                        Expressions.template(Integer.class, topTemplate, mod.getLimit())));
                context.serializeForQuery(metadata, forCountRow);
            } else {
                context.append(outerQueryStart);
                metadata = metadata.clone();
                RowNumber rn = new RowNumber();
                for (OrderSpecifier<?> os : metadata.getOrderBy()) {
                    rn.orderBy(os);
                }
                metadata.addProjection(rn.as(SQLServerGrammar.rowNumber));
                metadata.clearOrderBy();
                context.serializeForQuery(metadata, forCountRow);
                context.append(outerQueryEnd);
                if (mod.getLimit() == null) {
                    context.handle(offsetTemplate, mod.getOffset());
                } else if (mod.getOffset() == null) {
                    context.handle(limitTemplate, mod.getLimit());
                } else {
                    context.handle(limitOffsetTemplate, mod.getOffset(), mod.getLimit() + mod.getOffset());
                }
            }

        } else {
            context.serializeForQuery(metadata, forCountRow);
        }

        if (!metadata.getFlags().isEmpty()) {
            context.serialize(Position.END, metadata.getFlags());
        }
    }

}
