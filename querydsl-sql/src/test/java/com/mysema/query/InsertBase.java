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
package com.mysema.query;

import static com.mysema.query.Constants.survey;
import static com.mysema.query.Constants.survey2;
import static com.mysema.query.Target.CUBRID;
import static com.mysema.query.Target.DERBY;
import static com.mysema.query.Target.HSQLDB;
import static com.mysema.query.Target.MYSQL;
import static com.mysema.query.Target.ORACLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.mysema.query.QueryFlag.Position;
import com.mysema.query.sql.SQLSubQuery;
import com.mysema.query.sql.dml.DefaultMapper;
import com.mysema.query.sql.dml.Mapper;
import com.mysema.query.sql.dml.SQLInsertClause;
import com.mysema.query.sql.domain.Employee;
import com.mysema.query.sql.domain.QEmployee;
import com.mysema.query.sql.domain.QSurvey;
import com.mysema.query.support.Expressions;
import com.mysema.query.types.Path;
import com.mysema.query.types.PathImpl;
import com.mysema.query.types.expr.Param;
import com.mysema.testutil.ExcludeIn;
import com.mysema.testutil.IncludeIn;

public class InsertBase extends AbstractBaseTest {

    private void reset() throws SQLException{
        delete(survey).execute();
        insert(survey).values(1, "Hello World", "Hello").execute();
    }

    @Before
    public void setUp() throws SQLException{
        reset();
    }

    @After
    public void tearDown() throws SQLException{
        reset();
    }

    @Test
    public void Complex1() {
        // related to #584795
        QSurvey survey = new QSurvey("survey");
        QEmployee emp1 = new QEmployee("emp1");
        QEmployee emp2 = new QEmployee("emp2");
        SQLInsertClause insert = insert(survey);
        insert.columns(survey.id, survey.name);
        insert.select(new SQLSubQuery().from(survey)
          .innerJoin(emp1)
           .on(survey.id.eq(emp1.id))
          .innerJoin(emp2)
           .on(emp1.superiorId.eq(emp2.superiorId), emp1.firstname.eq(emp2.firstname))
          .list(survey.id, emp2.firstname));

        insert.execute();
    }

    @Test
    public void Insert_Alternative_Syntax() {
        // with columns
        assertEquals(1, insert(survey)
            .set(survey.id, 3)
            .set(survey.name, "Hello")
            .execute());
    }

    @Test
    public void Insert_Batch() {
        SQLInsertClause insert = insert(survey)
            .set(survey.id, 5)
            .set(survey.name, "55")
            .addBatch();

        insert.set(survey.id, 6)
            .set(survey.name, "66")
            .addBatch();

        assertEquals(2, insert.execute());

        assertEquals(1l, query().from(survey).where(survey.name.eq("55")).count());
        assertEquals(1l, query().from(survey).where(survey.name.eq("66")).count());
    }

    @Test
    public void Insert_Null_With_Columns() {
        assertEquals(1, insert(survey)
                .columns(survey.id, survey.name)
                .values(3, null).execute());
    }

    @Test
    @ExcludeIn(DERBY)
    public void Insert_Null_Without_Columns() {
        assertEquals(1, insert(survey)
                .values(4, null, null).execute());
    }

    @Test
    @ExcludeIn(ORACLE)
    public void Insert_Nulls_In_Batch() {
//        QFoo f= QFoo.foo;
//        SQLInsertClause sic = new SQLInsertClause(c, new H2Templates(), f);
//        sic.columns(f.c1,f.c2).values(null,null).addBatch();
//        sic.columns(f.c1,f.c2).values(null,1).addBatch();
//        sic.execute();
        SQLInsertClause sic = insert(survey);
        sic.columns(survey.name, survey.name2).values(null, null).addBatch();
        sic.columns(survey.name, survey.name2).values(null, "X").addBatch();
        sic.execute();
    }

    @Test
    @Ignore
    @ExcludeIn({DERBY})
    public void Insert_Nulls_In_Batch2() {
        Mapper<Object> mapper = DefaultMapper.WITH_NULL_BINDINGS;
//        QFoo f= QFoo.foo;
//        SQLInsertClause sic = new SQLInsertClause(c, new H2Templates(), f);
//        Foo f1=new Foo();
//        sic.populate(f1).addBatch();
//        f1=new Foo();
//        f1.setC1(1);
//        sic.populate(f1).addBatch();
//        sic.execute();
        QEmployee employee = QEmployee.employee;
        SQLInsertClause sic = insert(employee);
        Employee e = new Employee();
        sic.populate(e, mapper).addBatch();
        e = new Employee();
        e.setFirstname("X");
        sic.populate(e, mapper).addBatch();
        sic.execute();

    }

    @Test
    public void Insert_With_Columns() {
        assertEquals(1, insert(survey)
                .columns(survey.id, survey.name)
                .values(3, "Hello").execute());
    }

    @Test
    @ExcludeIn(CUBRID)
    public void Insert_With_Keys() throws SQLException{
        ResultSet rs = insert(survey).set(survey.name, "Hello World").executeWithKeys();
        assertTrue(rs.next());
        assertTrue(rs.getObject(1) != null);
        rs.close();
    }

    @Test
    @ExcludeIn(CUBRID)
    public void Insert_With_Keys_Projected() throws SQLException{
        assertNotNull(insert(survey).set(survey.name, "Hello you").executeWithKey(survey.id));
    }

    @Test
    @ExcludeIn(CUBRID)
    public void Insert_With_Keys_Projected2() throws SQLException{
        Path<Object> idPath = new PathImpl<Object>(Object.class, "id");
        Object id = insert(survey).set(survey.name, "Hello you").executeWithKey(idPath);
        assertNotNull(id);
    }

 // http://sourceforge.net/tracker/index.php?func=detail&aid=3513432&group_id=280608&atid=2377440

    @Test
    public void Insert_With_Set() {
        assertEquals(1, insert(survey)
                .set(survey.id, 5)
                .set(survey.name, (String)null)
                .execute());
    }

    @Test
    @IncludeIn(MYSQL)
    @SkipForQuoted
    public void Insert_with_Special_Options() {
        SQLInsertClause clause = insert(survey)
            .columns(survey.id, survey.name)
            .values(3, "Hello");

        clause.addFlag(Position.START_OVERRIDE, "insert ignore into ");

        assertEquals("insert ignore into SURVEY (ID, NAME) values (?, ?)", clause.toString());
        clause.execute();
    }

    @Test
    public void Insert_With_SubQuery() {
        int count = (int)query().from(survey).count();
        assertEquals(count, insert(survey)
            .columns(survey.id, survey.name)
            .select(sq().from(survey2).list(survey2.id.add(20), survey2.name))
            .execute());
    }

    @Test
    @ExcludeIn({HSQLDB, DERBY})
    public void Insert_With_SubQuery2() {
//        insert into modules(name)
//        select 'MyModule'
//        where not exists
//        (select 1 from modules where modules.name = 'MyModule')

        assertEquals(1, insert(survey).set(survey.name,
            sq().where(sq().from(survey2)
                           .where(survey2.name.eq("MyModule")).notExists())
                .unique(Expressions.constant("MyModule")))
            .execute());

        assertEquals(1l , query().from(survey).where(survey.name.eq("MyModule")).count());
    }

    @Test
    @ExcludeIn({HSQLDB, DERBY})
    public void Insert_With_SubQuery3() {
//        insert into modules(name)
//        select 'MyModule'
//        where not exists
//        (select 1 from modules where modules.name = 'MyModule')

        assertEquals(1, insert(survey).columns(survey.name).select(
            sq().where(sq().from(survey2)
                           .where(survey2.name.eq("MyModule2")).notExists())
                .unique(Expressions.constant("MyModule2")))
            .execute());

        assertEquals(1l , query().from(survey).where(survey.name.eq("MyModule2")).count());
    }

    @Test
    public void Insert_With_SubQuery_Params() {
        Param<Integer> param = new Param<Integer>(Integer.class, "param");
        SQLSubQuery sq = sq().from(survey2);
        sq.set(param, 20);

        int count = (int)query().from(survey).count();
        assertEquals(count, insert(survey)
            .columns(survey.id, survey.name)
            .select(sq.list(survey2.id.add(param), survey2.name))
            .execute());
    }

    @Test
    public void Insert_With_SubQuery_Via_Constructor() {
        int count = (int)query().from(survey).count();
        SQLInsertClause insert = insert(survey, sq().from(survey2));
        insert.set(survey.id, survey2.id.add(20));
        insert.set(survey.name, survey2.name);
        assertEquals(count, insert.execute());
    }

    @Test
    public void Insert_With_SubQuery_Without_Columns() {
        int count = (int)query().from(survey).count();
        assertEquals(count, insert(survey)
            .select(sq().from(survey2).list(survey2.id.add(10), survey2.name, survey2.name2))
            .execute());

    }

    @Test
    public void Insert_Without_Columns() {
        assertEquals(1, insert(survey).values(4, "Hello", "World").execute());

    }

    @Test
    public void InsertBatch_with_Subquery() {
        SQLInsertClause insert = insert(survey)
            .columns(survey.id, survey.name)
            .select(sq().from(survey2).list(survey2.id.add(20), survey2.name))
            .addBatch();

        insert(survey)
            .columns(survey.id, survey.name)
            .select(sq().from(survey2).list(survey2.id.add(40), survey2.name))
            .addBatch();

        insert.execute();
//        assertEquals(1, insert.execute());
    }

    @Test
    public void Like() {
        insert(survey).values(11, "Hello World", "a\\b").execute();
        assertEquals(1l, query().from(survey).where(survey.name2.contains("a\\b")).count());
    }

    @Test
    public void Like_with_Escape() {
        SQLInsertClause insert = insert(survey);
        insert.set(survey.id, 5).set(survey.name, "aaa").addBatch();
        insert.set(survey.id, 6).set(survey.name, "a_").addBatch();
        insert.set(survey.id, 7).set(survey.name, "a%").addBatch();
        assertEquals(3, insert.execute());

        assertEquals(1l, query().from(survey).where(survey.name.like("a|%", '|')).count());
        assertEquals(1l, query().from(survey).where(survey.name.like("a|_", '|')).count());
        assertEquals(3l, query().from(survey).where(survey.name.like("a%")).count());
        assertEquals(2l, query().from(survey).where(survey.name.like("a_")).count());

        assertEquals(1l, query().from(survey).where(survey.name.startsWith("a_")).count());
        assertEquals(1l, query().from(survey).where(survey.name.startsWith("a%")).count());
    }

    @Test
    @IncludeIn(MYSQL)
    @SkipForQuoted
    public void Replace() {
        SQLInsertClause clause = mysqlReplace(survey);
        clause.columns(survey.id, survey.name)
            .values(3, "Hello");

        assertEquals("replace into SURVEY (ID, NAME) values (?, ?)", clause.toString());
        clause.execute();
    }


}
