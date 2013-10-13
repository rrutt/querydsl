package com.mysema.query.suites;

import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.mysema.query.HibernateBase;
import com.mysema.query.HibernateSQLBase;
import com.mysema.query.JPABase;
import com.mysema.query.JPASQLBase;
import com.mysema.query.Mode;
import com.mysema.query.jpa.JPAIntegrationBase;
import com.mysema.query.jpa.SerializationBase;

@RunWith(Suite.class)
@SuiteClasses({
    JPABase.class,
    JPASQLBase.class,
    JPAIntegrationBase.class,
    SerializationBase.class,
    HibernateBase.class,
    HibernateSQLBase.class})
public abstract class AbstractSuite {

    @BeforeClass
    public static void tearDown() throws Exception {
        Mode.mode.remove();
        Mode.target.remove();
    }

}
