package com.mysema.query.jpa.domain11;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.hibernate.cfg.Configuration;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.mysema.query.jpa.codegen.HibernateDomainExporter;
import com.mysema.util.FileUtils;

public class DomainExporterTest {

    @Test
    public void Execute() throws IOException {
        File gen = new File("target/" + getClass().getSimpleName());
        FileUtils.delete(gen);
        Configuration config = new Configuration();
        config.addFile(new File("src/test/resources/com/mysema/query/jpa/domain11/domain.hbm.xml"));
        HibernateDomainExporter exporter = new HibernateDomainExporter("Q", gen, config);
        exporter.execute();
        
        assertTrue(new File(gen, "com/mysema/query/jpa/domain11/QOtherthing.java").exists());
        assertTrue(new File(gen, "com/mysema/query/jpa/domain11/QSomething.java").exists());
        
        String str = Files.toString(new File(gen, "com/mysema/query/jpa/domain11/QOtherthing.java"), Charsets.UTF_8);
        assertTrue(str.contains("QSomething"));
        
        str = Files.toString(new File(gen, "com/mysema/query/jpa/domain11/QSomething.java"), Charsets.UTF_8);
        assertTrue(str.contains("id"));
    }
    
}
