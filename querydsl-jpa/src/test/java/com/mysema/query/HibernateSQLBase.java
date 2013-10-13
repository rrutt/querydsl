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

import static org.junit.Assert.assertEquals;

import org.hibernate.Query;
import org.hibernate.Session;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.MethodRule;
import org.junit.runner.RunWith;

import com.mysema.query.jpa.domain.Cat;
import com.mysema.query.jpa.domain.QCat;
import com.mysema.query.jpa.domain.sql.SAnimal;
import com.mysema.query.jpa.hibernate.sql.HibernateSQLQuery;
import com.mysema.query.sql.SQLTemplates;
import com.mysema.testutil.ExcludeIn;
import com.mysema.testutil.HibernateTestRunner;

@RunWith(HibernateTestRunner.class)
public class HibernateSQLBase extends AbstractSQLTest {

    @Rule
    public static MethodRule targetRule = new TargetRule();

    private final SQLTemplates templates = Mode.getSQLTemplates();

    private final SAnimal cat = new SAnimal("cat");

    private Session session;

    @Override
    protected HibernateSQLQuery query() {
        return new HibernateSQLQuery(session, templates);
    }

    public void setSession(Session session) {
        this.session = session;
    }

    @Before
    public void setUp() {
        if (query().from(cat).notExists()) {
            session.save(new Cat("Beck",1));
            session.save(new Cat("Kate",2));
            session.save(new Cat("Kitty",3));
            session.save(new Cat("Bobby",4));
            session.save(new Cat("Harold",5));
            session.save(new Cat("Tim",6));
            session.flush();
        }
    }

    @Test
    public void EntityQueries_CreateQuery() {
        SAnimal cat = new SAnimal("cat");
        QCat catEntity = QCat.cat;

        Query query = query().from(cat).createQuery(catEntity);
        assertEquals(6, query.list().size());
    }

    @Test
    @ExcludeIn(Target.MYSQL)
    public void EntityQueries_CreateQuery2() {
        SAnimal cat = new SAnimal("CAT");
        QCat catEntity = QCat.cat;

        Query query = query().from(cat).createQuery(catEntity);
        assertEquals(6, query.list().size());
    }


}
