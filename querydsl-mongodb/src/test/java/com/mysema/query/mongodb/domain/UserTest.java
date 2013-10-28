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
package com.mysema.query.mongodb.domain;

import org.junit.Test;
import org.mongodb.morphia.Morphia;

public class UserTest {
    
    @Test
    public void Map() {
        City tampere = new City("Tampere", 61.30, 23.50);
        
        User user = new User();
        user.setAge(12);
        user.setFirstName("Jaakko");        
        user.addAddress("Aakatu", "00300", tampere);
        
        System.out.println(new Morphia().map(User.class).toDBObject(user));
    }

}
