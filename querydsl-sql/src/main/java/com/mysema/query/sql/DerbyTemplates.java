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

import com.mysema.query.QueryMetadata;
import com.mysema.query.QueryModifiers;
import com.mysema.query.types.Ops;

/**
 * DerbyTemplates is an SQL dialect for Derby
 *
 * @author tiwe
 *
 */
public class DerbyTemplates extends SQLTemplates {

    private String limitOffsetTemplate = "\noffset {1s} rows fetch next {0s} rows only";

    private String limitTemplate = "\nfetch first {0s} rows only";

    private String offsetTemplate = "\noffset {0s} rows";

    public static Builder builder() {
        return new Builder() {
            @Override
            protected SQLTemplates build(char escape, boolean quote) {
                return new DerbyTemplates(escape, quote);
            }
        };
    }

    public DerbyTemplates() {
        this('\\',false);
    }

    public DerbyTemplates(boolean quote) {
        this('\\',quote);
    }

    public DerbyTemplates(char escape, boolean quote) {
        super("\"", escape, quote);
        setDummyTable("sysibm.sysdummy1");
        addClass2TypeMappings("smallint", Byte.class);
        setAutoIncrement(" generated always as identity");
        setFunctionJoinsWrapped(true);

        add(Ops.CONCAT, "varchar({0} || {1})");
        add(Ops.DateTimeOps.DAY_OF_MONTH, "day({0})");

        add(NEXTVAL, "next value for {0s}");

        // case for eq
        add(Ops.CASE_EQ, "case {1} end");
        add(Ops.CASE_EQ_WHEN,  "when {0} = {1} then {2} {3}");
        add(Ops.CASE_EQ_ELSE,  "else {0}");

        add(Ops.MathOps.RANDOM, "random()");
        add(Ops.MathOps.ROUND, "floor({0})"); // FIXME
        add(Ops.MathOps.POWER, "exp({1} * log({0}))");
        add(Ops.MathOps.LN, "log({0})");
        add(Ops.MathOps.LOG, "(log({0}) / log({1}))");
        add(Ops.MathOps.COTH, "(exp({0} * 2) + 1) / (exp({0} * 2) - 1)");

//        add(Ops.DateTimeOps.DATE_ADD, "date_add({0}, INTERVAL {1} {2s})");
        add(Ops.DateTimeOps.ADD_YEARS, "{fn timestampadd(SQL_TSI_YEAR, {1}, {0})}");
        add(Ops.DateTimeOps.ADD_MONTHS, "{fn timestampadd(SQL_TSI_MONTH, {1}, {0})}");
        add(Ops.DateTimeOps.ADD_WEEKS, "{fn timestampadd(SQL_TSI_WEEK, {1}, {0})}");
        add(Ops.DateTimeOps.ADD_DAYS, "{fn timestampadd(SQL_TSI_DAY, {1}, {0})}");
        add(Ops.DateTimeOps.ADD_HOURS, "{fn timestampadd(SQL_TSI_HOUR, {1}, {0})}");
        add(Ops.DateTimeOps.ADD_MINUTES, "{fn timestampadd(SQL_TSI_MINUTE, {1}, {0})}");
        add(Ops.DateTimeOps.ADD_SECONDS, "{fn timestampadd(SQL_TSI_SECOND, {1}, {0})}");

        add(Ops.DateTimeOps.DIFF_YEARS, "{fn timestampdiff(SQL_TSI_YEAR, {0}, {1})}");
        add(Ops.DateTimeOps.DIFF_MONTHS, "{fn timestampdiff(SQL_TSI_MONTH, {0}, {1})}");
        add(Ops.DateTimeOps.DIFF_WEEKS, "{fn timestampdiff(SQL_TSI_WEEK, {0}, {1})}");
        add(Ops.DateTimeOps.DIFF_DAYS, "{fn timestampdiff(SQL_TSI_DAY, {0}, {1})}");
        add(Ops.DateTimeOps.DIFF_HOURS, "{fn timestampdiff(SQL_TSI_HOUR, {0}, {1})}");
        add(Ops.DateTimeOps.DIFF_MINUTES, "{fn timestampdiff(SQL_TSI_MINUTE, {0}, {1})}");
        add(Ops.DateTimeOps.DIFF_SECONDS, "{fn timestampdiff(SQL_TSI_SECOND, {0}, {1})}");

        // left via substr
        add(Ops.StringOps.LEFT, "substr({0},1,{1})");
    }

    @Override
    protected void serializeModifiers(QueryMetadata metadata, SQLSerializer context) {
        QueryModifiers mod = metadata.getModifiers();
        if (mod.getLimit() == null) {
            context.handle(offsetTemplate, mod.getOffset());
        } else if (mod.getOffset() == null) {
            context.handle(limitTemplate, mod.getLimit());
        } else {
            context.handle(limitOffsetTemplate, mod.getLimit(), mod.getOffset());
        }
    }

}
