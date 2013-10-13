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
package com.mysema.query.domain;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import org.junit.Test;

import com.mysema.query.annotations.Config;
import com.mysema.query.annotations.PropertyType;
import com.mysema.query.annotations.QueryEntity;
import com.mysema.query.annotations.QueryTransient;
import com.mysema.query.annotations.QueryType;
import com.mysema.query.types.path.ComparablePath;
import com.mysema.query.types.path.DateTimePath;
import com.mysema.query.types.path.EnumPath;
import com.mysema.query.types.path.NumberPath;
import com.mysema.query.types.path.SimplePath;
import com.mysema.query.types.path.StringPath;
import com.mysema.query.types.path.TimePath;

public class SimpleTypesTest extends AbstractTest {

    public enum MyEnum {
        VAL1,
        VAL2
    }

    public static class CustomLiteral {

    }

    @SuppressWarnings("serial")
    public static class CustomNumber extends Number {

        @Override
        public double doubleValue() {
            return 0;
        }

        @Override
        public float floatValue() {
            return 0;
        }

        @Override
        public int intValue() {
            return 0;
        }

        @Override
        public long longValue() {
            return 0;
        }

    }

    public static class CustomComparableNumber extends CustomNumber implements Comparable<CustomComparableNumber> {

        private static final long serialVersionUID = 4398583038967396133L;

        @Override
        public int compareTo(CustomComparableNumber o) {
            return 0;
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof CustomComparableNumber;
        }
    }

    public static class CustomComparableLiteral implements Comparable<CustomComparableLiteral> {

        @Override
        public int compareTo(CustomComparableLiteral o) {
            return 0;
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof CustomComparableLiteral;
        }
    }

    public static class CustomGenericComparableLiteral<C> implements Comparable<CustomComparableLiteral> {

        @Override
        public int compareTo(CustomComparableLiteral o) {
            return 0;
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof CustomGenericComparableLiteral;
        }
    }

    @QueryEntity
    @Config(listAccessors=true)
    public static class SimpleTypes {
        transient int test;
        List<Integer> testList;

        Calendar calendar;
        List<Calendar> calendarList;

        long id;
        List<Long> idList;

        BigDecimal bigDecimal;
        List<BigDecimal> bigDecimalList;

        Byte bbyte;
        List<Byte> bbyteList;
        byte bbyte2;

        Short sshort;
        List<Short> sshortList;
        short sshort2;

        Character cchar;
        List<Character> ccharList;
        char cchar2;

        Double ddouble;
        List<Double> ddoubleList;
        double ddouble2;

        Float ffloat;
        List<Float> ffloatList;
        float ffloat2;

        Integer iint;
        List<Integer> iintList;
        int iint2;

        Locale llocale;
        List<Locale> llocaleList;

        Long llong;
        List<Long> llongList;
        long llong2;

        BigInteger bigInteger;

        String sstring;
        List<String> sstringList;

        Date date;
        List<Date> dateList;

        java.sql.Time time;
        List<java.sql.Time> timeList;

        java.sql.Timestamp timestamp;
        List<java.sql.Timestamp> timestampList;

        Serializable serializable;
        List<Serializable> serializableList;

        Object object;
        List<Object> objectList;

        Class<?> clazz;
        List<Class> classList2;
        List<Class<?>> classList3;
        List<Class<Package>> classList4;
        List<Class<? extends Date>> classList5;

        Package packageAsLiteral;
        List<Package> packageAsLiteralList;

        CustomLiteral customLiteral;
        List<CustomLiteral> customLiteralList;

        CustomComparableLiteral customComparableLiteral;
        List<CustomComparableLiteral> customComparableLiteralList;

        CustomNumber customNumber;
        List<CustomNumber> customNumberList;

        CustomComparableNumber customComparableNumber;
        List<CustomComparableNumber> customComparableNumber2;

        CustomGenericComparableLiteral customComparableLiteral2;
        List<CustomGenericComparableLiteral> customComparableLiteral2List;

        CustomGenericComparableLiteral<Number> customComparableLiteral3;
        List<CustomGenericComparableLiteral<Number>> customComparableLiteral3List;

        java.sql.Clob clob;
        List<java.sql.Clob> clobList;

        java.sql.Blob blob;
        List<java.sql.Blob> blobList;

        @QueryTransient
        String skipMe;

        MyEnum myEnum;

        int[] intArray;
        byte[] byteArray;
        long[] longArray;
        float[] floatArray;
        double[] doubleArray;
        short[] shortArray;

        @QueryType(PropertyType.SIMPLE)
        byte[] byteArrayAsSimple;
    }

    @Test
    public void List_Access() {
        // date / time
        QSimpleTypesTest_SimpleTypes.simpleTypes.dateList.get(0).after(new Date());
        QSimpleTypesTest_SimpleTypes.simpleTypes.timeList.get(0).after(new Time(0l));
        QSimpleTypesTest_SimpleTypes.simpleTypes.calendarList.get(0).before(Calendar.getInstance());

        // numeric
        QSimpleTypesTest_SimpleTypes.simpleTypes.bbyteList.get(0).abs();

        // string
        QSimpleTypesTest_SimpleTypes.simpleTypes.sstringList.get(0).toLowerCase();

        // boolean
//        QSimpleTypes.simpleTypes.b

    }

    @Test
    public void Simple_Types() throws SecurityException, NoSuchFieldException {
        cl = QSimpleTypesTest_SimpleTypes.class;
        match(NumberPath.class, "id");
        match(NumberPath.class, "bigDecimal");
        match(NumberPath.class, "bigInteger");
//        match(PNumber.class, "bbyte");
        match(NumberPath.class, "bbyte2");
        match(NumberPath.class, "ddouble");
        match(NumberPath.class, "ddouble2");
        match(NumberPath.class, "ffloat");
        match(NumberPath.class, "ffloat2");
//        match(PNumber.class, "iint");
        match(NumberPath.class, "iint2");
        match(NumberPath.class, "llong");
        match(NumberPath.class, "llong2");

        match(ComparablePath.class, "cchar");
        match(ComparablePath.class, "cchar2");

        match(StringPath.class, "sstring");

        match(DateTimePath.class, "date");
        match(DateTimePath.class, "calendar");
//        match(PDateTime.class, "timestamp");

        match(TimePath.class, "time");

        match(SimplePath.class, "llocale");
        match(SimplePath.class, "serializable");
        match(SimplePath.class, "object");
        match(SimplePath.class, "clazz");
        match(SimplePath.class, "packageAsLiteral");

        match(SimplePath.class, "clob");
        match(SimplePath.class, "blob");

        match(EnumPath.class, "myEnum");
    }

    @Test
    public void Custom_Literal() throws SecurityException, NoSuchFieldException {
        cl = QSimpleTypesTest_SimpleTypes.class;
        match(SimplePath.class, "customLiteral");
    }

    @Test
    public void Custom_ComparableLiteral() throws SecurityException, NoSuchFieldException {
        cl = QSimpleTypesTest_SimpleTypes.class;
        match(ComparablePath.class, "customComparableLiteral");
    }

    @Test
    public void Custom_Number() throws SecurityException, NoSuchFieldException {
        cl = QSimpleTypesTest_SimpleTypes.class;
        match(SimplePath.class, "customNumber");
    }

    @Test
    public void Custom_ComparableNumber() throws SecurityException, NoSuchFieldException {
        cl = QSimpleTypesTest_SimpleTypes.class;
        match(NumberPath.class, "customComparableNumber");
    }

    @Test(expected=NoSuchFieldException.class)
    public void Skipped_Field1() throws SecurityException, NoSuchFieldException {
        QSimpleTypesTest_SimpleTypes.class.getField("skipMe");
    }

    @Test(expected=NoSuchFieldException.class)
    public void Skipped_Field2() throws SecurityException, NoSuchFieldException {
        QSimpleTypesTest_SimpleTypes.class.getField("test");
    }

}
