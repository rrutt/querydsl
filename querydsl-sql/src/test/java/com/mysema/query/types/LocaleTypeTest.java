package com.mysema.query.types;

import static org.junit.Assert.assertEquals;

import java.util.Locale;

import org.junit.Test;

import com.mysema.query.sql.types.LocaleType;

public class LocaleTypeTest {

    @Test
    public void Lang() {
        Locale l = new Locale("en");
        assertEquals(l, LocaleType.toLocale(l.toString()));
    }

    @Test
    public void Lang_Country() {
        Locale l = new Locale("en", "US");
        assertEquals(l, LocaleType.toLocale(l.toString()));
    }

    @Test
    public void Lang_Country_Variant() {
        Locale l = new Locale("en", "US", "X");
        assertEquals(l, LocaleType.toLocale(l.toString()));
    }

}
