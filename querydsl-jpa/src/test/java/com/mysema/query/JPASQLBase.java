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

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.MethodRule;
import org.junit.runner.RunWith;

import com.mysema.query.jpa.domain.Cat;
import com.mysema.query.jpa.domain.QCat;
import com.mysema.query.jpa.domain.sql.SAnimal;
import com.mysema.query.jpa.sql.JPASQLQuery;
import com.mysema.query.sql.SQLTemplates;
import com.mysema.testutil.ExcludeIn;
import com.mysema.testutil.JPATestRunner;

@RunWith(JPATestRunner.class)
public class JPASQLBase extends AbstractSQLTest {

    @Rule
    public static MethodRule targetRule = new TargetRule();

    @Rule
    public static MethodRule hibernateOnly = new JPAProviderRule();

    private final SQLTemplates templates = Mode.getSQLTemplates();

    private EntityManager entityManager;

    @Override
    protected JPASQLQuery query() {
        return new JPASQLQuery(entityManager, templates);
    }


    public void setEntityManager(EntityManager entityManager) {
        this.entityManager = entityManager;
    }

    @Before
    public void setUp() {
        if (query().from(cat).notExists()) {
            entityManager.persist(new Cat("Beck", 1));
            entityManager.persist(new Cat("Kate", 2));
            entityManager.persist(new Cat("Kitty", 3));
            entityManager.persist(new Cat("Bobby", 4));
            entityManager.persist(new Cat("Harold", 5));
            entityManager.persist(new Cat("Tim", 6));
            entityManager.flush();
        }
    }

    @Test
    public void EntityQueries_CreateQuery() {
        SAnimal cat = new SAnimal("cat");
        QCat catEntity = QCat.cat;

        Query query = query().from(cat).createQuery(catEntity);
        assertEquals(6, query.getResultList().size());
    }

    @Test
    @ExcludeIn(Target.MYSQL)
    public void EntityQueries_CreateQuery2() {
        SAnimal cat = new SAnimal("CAT");
        QCat catEntity = QCat.cat;

        Query query = query().from(cat).createQuery(catEntity);
        assertEquals(6, query.getResultList().size());
    }

}
