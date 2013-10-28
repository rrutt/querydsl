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
package com.mysema.query.mongodb;

import static org.junit.Assert.assertEquals;

import java.net.UnknownHostException;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Morphia;

import com.mongodb.BasicDBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mysema.query.mongodb.domain.GeoEntity;
import com.mysema.query.mongodb.domain.QGeoEntity;
import com.mysema.query.mongodb.morphia.MorphiaQuery;
import com.mysema.testutil.ExternalDB;

@Category(ExternalDB.class)
public class GeoSpatialQueryTest {

    private final String dbname = "geodb";
    private final Mongo mongo;
    private final Morphia morphia;
    private final Datastore ds;
    private final QGeoEntity geoEntity = new QGeoEntity("geoEntity");

    public GeoSpatialQueryTest() throws UnknownHostException, MongoException {
        mongo = new Mongo();
        morphia = new Morphia().map(GeoEntity.class);
        ds = morphia.createDatastore(mongo, dbname);
    }

    @Before
    public void before() {
        ds.delete(ds.createQuery(GeoEntity.class));
        ds.getCollection(GeoEntity.class).ensureIndex(new BasicDBObject("location","2d"));;
    }

    @Test
    public void Near() {
        ds.save(new GeoEntity(10.0, 50.0));
        ds.save(new GeoEntity(20.0, 50.0));
        ds.save(new GeoEntity(30.0, 50.0));

        List<GeoEntity> entities = query().where(geoEntity.location.near(50.0, 50.0)).list();
        assertEquals(30.0, entities.get(0).getLocation()[0].doubleValue(), 0.1);
        assertEquals(20.0, entities.get(1).getLocation()[0].doubleValue(), 0.1);
        assertEquals(10.0, entities.get(2).getLocation()[0].doubleValue(), 0.1);
    }

    private MongodbQuery<GeoEntity> query() {
        return new MorphiaQuery<GeoEntity>(morphia, ds, geoEntity);
    }

}
