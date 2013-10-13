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
package com.mysema.query.codegen;

import java.lang.annotation.Annotation;
import java.lang.reflect.Array;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.primitives.Primitives;
import com.mysema.codegen.model.ClassType;
import com.mysema.codegen.model.SimpleType;
import com.mysema.codegen.model.Type;
import com.mysema.codegen.model.TypeCategory;
import com.mysema.codegen.model.TypeExtends;
import com.mysema.codegen.model.TypeSuper;
import com.mysema.codegen.model.Types;
import com.mysema.util.ReflectionUtils;

/**
 * TypeFactory is a factory class for {@link Type} instances
 *
 * @author tiwe
 *
 */
public final class TypeFactory {

    private static final Type ANY = new TypeExtends(Types.OBJECT);

    private final Map<List<java.lang.reflect.Type>, Type> cache = new HashMap<List<java.lang.reflect.Type>, Type>();

    private final List<Class<? extends Annotation>> entityAnnotations;

    private final Set<Class<?>> embeddableTypes = new HashSet<Class<?>>();

    private boolean unknownAsEntity = false;

    public TypeFactory() {
        this(Lists.<Class<? extends Annotation>>newArrayList());
    }

    public TypeFactory(List<Class<? extends Annotation>> entityAnnotations) {
        this.entityAnnotations = entityAnnotations;
    }

    public EntityType getEntityType(Class<?> cl) {
        java.lang.reflect.Type generic = cl;
        if (cl.getTypeParameters().length > 0) {
            generic = new ParameterizedTypeImpl(cl, cl.getTypeParameters());
        }
        return (EntityType) get(true, cl, generic);
    }

    public Type get(Class<?> cl) {
        return get(isEntityClass(cl), cl, cl);
    }

    public Type get(Class<?> cl, java.lang.reflect.Type genericType) {
        return get(isEntityClass(cl), cl, genericType);
    }

    public Type get(boolean entity, Class<?> cl, java.lang.reflect.Type genericType) {
        List<java.lang.reflect.Type> key = Arrays.<java.lang.reflect.Type> asList(cl, genericType);
        if (cache.containsKey(key)) {
            Type value = cache.get(key);
            if (entity && !(value instanceof EntityType)) {
                value = new EntityType(value);
                cache.put(key, value);
            }
            return value;

        } else {
            Type value = create(entity, cl, genericType, key);
            cache.put(key, value);
            return value;
        }
    }

    private Type create(boolean entity, Class<?> cl, java.lang.reflect.Type genericType,
            List<java.lang.reflect.Type> key) {
        if (cl.isPrimitive()) {
            cl = Primitives.wrap(cl);
        }
        Type value;
        Type[] tempParams = (Type[]) Array.newInstance(Type.class,
                ReflectionUtils.getTypeParameterCount(genericType));
        cache.put(key, new ClassType(cl, tempParams));
        Type[] parameters = getParameters(cl, genericType);

        if (cl.isArray()) {
            Type componentType = get(cl.getComponentType());
            if (cl.getComponentType().isPrimitive()) {
                componentType = Types.PRIMITIVES.get(componentType);
            }
            value = componentType.asArrayType();
        } else if (cl.isEnum()) {
            value = new ClassType(TypeCategory.ENUM, cl);
        } else if (Number.class.isAssignableFrom(cl) && Comparable.class.isAssignableFrom(cl)) {
            value = new ClassType(TypeCategory.NUMERIC, cl, parameters);
        } else if (entity) {
            value = createOther(cl, entity, parameters);
        } else if (Map.class.isAssignableFrom(cl)) {
            value = new SimpleType(Types.MAP, parameters[0], asGeneric(parameters[1]));
        } else if (List.class.isAssignableFrom(cl)) {
            value = new SimpleType(Types.LIST, asGeneric(parameters[0]));
        } else if (Set.class.isAssignableFrom(cl)) {
            value = new SimpleType(Types.SET, asGeneric(parameters[0]));
        } else if (Collection.class.isAssignableFrom(cl)) {
            value = new SimpleType(Types.COLLECTION, asGeneric(parameters[0]));
        } else {
            value = createOther(cl, entity, parameters);
        }

        if (genericType instanceof TypeVariable) {
            TypeVariable tv = (TypeVariable)genericType;
            if (tv.getBounds().length == 1 && tv.getBounds()[0].equals(Object.class)) {
                value = new TypeSuper(tv.getName(), value);
            } else {
                value = new TypeExtends(tv.getName(), value);
            }
        }

        if (entity && !(value instanceof EntityType)) {
            value = new EntityType(value);
        }
        return value;
    }

    private Type asGeneric(Type type) {
        if (type.getParameters().size() == 0) {
            int count = type.getJavaClass().getTypeParameters().length;
            if (count > 0) {
                return new SimpleType(type, new Type[count]);
            }
        }
        return type;
    }

    private Type createOther(Class<?> cl, boolean entity, Type[] parameters) {
        TypeCategory typeCategory = TypeCategory.get(cl.getName());
        if (!typeCategory.isSubCategoryOf(TypeCategory.COMPARABLE) && Comparable.class.isAssignableFrom(cl)
            && !cl.equals(Comparable.class)) {
            typeCategory = TypeCategory.COMPARABLE;
        } else if (embeddableTypes.contains(cl)) {
            typeCategory = TypeCategory.CUSTOM;
        } else if (typeCategory == TypeCategory.SIMPLE && entity) {
            typeCategory = TypeCategory.ENTITY;
        } else if (unknownAsEntity && typeCategory == TypeCategory.SIMPLE && !cl.getName().startsWith("java")) {
            typeCategory = TypeCategory.CUSTOM;
        }
        return new ClassType(typeCategory, cl, parameters);
    }

    private Type[] getParameters(Class<?> cl, java.lang.reflect.Type genericType) {
        int parameterCount = ReflectionUtils.getTypeParameterCount(genericType);
        if (parameterCount > 0) {
            return getGenericParameters(cl, genericType, parameterCount);
        } else if (Map.class.isAssignableFrom(cl)) {
            return new Type[]{ Types.OBJECT, Types.OBJECT };
        } else if (Collection.class.isAssignableFrom(cl)) {
            return new Type[]{ Types.OBJECT };
        } else {
            return new Type[0];
        }
    }

    private Type[] getGenericParameters(Class<?> cl, java.lang.reflect.Type genericType,
            int parameterCount) {
        Type[] types = new Type[parameterCount];
        for (int i = 0; i < types.length; i++) {
            types[i] = getGenericParameter(cl, genericType, i);
        }
        return types;
    }

    @SuppressWarnings("rawtypes")
    private Type getGenericParameter(Class<?> cl, java.lang.reflect.Type genericType, int i) {
        java.lang.reflect.Type parameter = ReflectionUtils.getTypeParameter(genericType, i);
        if (parameter instanceof TypeVariable) {
            TypeVariable variable = (TypeVariable)parameter;
            Type rv = get(ReflectionUtils.getTypeParameterAsClass(genericType, i), parameter);
            return new TypeExtends(variable.getName(), rv);
        } else if (parameter instanceof WildcardType
            && ((WildcardType)parameter).getUpperBounds()[0].equals(Object.class)
            && ((WildcardType)parameter).getLowerBounds().length == 0) {
            return ANY;
        } else {
            Type rv = get(ReflectionUtils.getTypeParameterAsClass(genericType, i), parameter);
            if (parameter instanceof WildcardType) {
                rv = new TypeExtends(rv);
            }
            return rv;
        }
    }

    private boolean isEntityClass(Class<?> cl) {
        for (Class<? extends Annotation> clazz : entityAnnotations) {
            if (cl.getAnnotation(clazz) != null) {
                return true;
            }
        }
        return embeddableTypes.contains(cl);
    }

    public void extendTypes() {
        for (Map.Entry<List<java.lang.reflect.Type>, Type> entry : cache.entrySet()) {
            if (entry.getValue() instanceof EntityType) {
                EntityType entityType = (EntityType)entry.getValue();
                if (entityType.getProperties().isEmpty()) {
                    for (Type type : cache.values()) {
                        if (type.getFullName().equals(entityType.getFullName()) && type instanceof EntityType) {
                            EntityType base = (EntityType)type;
                            for (Property property : base.getProperties()) {
                                entityType.addProperty(property);
                            }
                        }
                    }
                }
            }
        }
    }

    public void setUnknownAsEntity(boolean unknownAsEntity) {
        this.unknownAsEntity = unknownAsEntity;
    }

    public void addEmbeddableType(Class<?> cl) {
        embeddableTypes.add(cl);
    }

}
