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
package com.mysema.testutil;

import java.lang.reflect.Method;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.rules.MethodRule;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;

import com.mysema.query.Mode;

/**
 * @author tiwe
 *
 */
public class JPATestRunner extends BlockJUnit4ClassRunner {

    private EntityManagerFactory entityManagerFactory;

    private EntityManager entityManager;

    private Method setter;

    private boolean isDerby;

    public JPATestRunner(Class<?> klass) throws InitializationError{
        super(klass);
    }

    @Override
    protected List<MethodRule> rules(Object test) {
        List<MethodRule> rules = super.rules(test);
        rules.add(new MethodRule() {
            @Override
            public Statement apply(final Statement base, FrameworkMethod method, final Object target) {
                return new Statement() {
                    @Override
                    public void evaluate() throws Throwable {
                        if (setter == null) {
                            setter = target.getClass().getMethod("setEntityManager", EntityManager.class);
                        }
                        setter.invoke(target, entityManager);
                        base.evaluate();
                    }
                };
            }

        });
        return rules;
    }

    @Override
    public void run(final RunNotifier notifier) {
        try {
            start();
            super.run(notifier);
        } catch (Exception e) {
            String error = "Caught " + e.getClass().getName();
            throw new RuntimeException(error, e);
        } finally {
            shutdown();
        }

    }

    private void start() throws Exception {
        String mode = Mode.mode.get();
        if (mode == null) {
            mode = "h2perf";
        }
        System.out.println(mode);
        isDerby = mode.contains("derby");
        if (isDerby) {
            Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
        }
        entityManagerFactory = Persistence.createEntityManagerFactory(mode);
        entityManager = entityManagerFactory.createEntityManager();
        entityManager.getTransaction().begin();
    }

    private void shutdown() {
        if (entityManager != null) {
            try {
                if (entityManager.getTransaction().isActive()) {
                    entityManager.getTransaction().rollback();
                }
            } finally {
                entityManager.close();
                entityManager = null;
            }
        }

        if (entityManagerFactory != null) {
            if (entityManagerFactory.getCache() != null) {
                entityManagerFactory.getCache().evictAll();
            }
            entityManagerFactory.close();
            entityManagerFactory = null;
        }

        // clean shutdown of derby
        if (isDerby) {
            try {
                DriverManager.getConnection("jdbc:derby:;shutdown=true");
            } catch (SQLException e) {
                if (!e.getMessage().equals("Derby system shutdown.")) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
