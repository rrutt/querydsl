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

import static com.mysema.codegen.Symbols.ASSIGN;
import static com.mysema.codegen.Symbols.COMMA;
import static com.mysema.codegen.Symbols.DOT;
import static com.mysema.codegen.Symbols.DOT_CLASS;
import static com.mysema.codegen.Symbols.EMPTY;
import static com.mysema.codegen.Symbols.NEW;
import static com.mysema.codegen.Symbols.QUOTE;
import static com.mysema.codegen.Symbols.RETURN;
import static com.mysema.codegen.Symbols.SEMICOLON;
import static com.mysema.codegen.Symbols.STAR;
import static com.mysema.codegen.Symbols.SUPER;
import static com.mysema.codegen.Symbols.THIS;
import static com.mysema.codegen.Symbols.UNCHECKED;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Generated;
import javax.inject.Inject;
import javax.inject.Named;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.mysema.codegen.CodeWriter;
import com.mysema.codegen.model.ClassType;
import com.mysema.codegen.model.Constructor;
import com.mysema.codegen.model.Parameter;
import com.mysema.codegen.model.SimpleType;
import com.mysema.codegen.model.Type;
import com.mysema.codegen.model.TypeCategory;
import com.mysema.codegen.model.TypeExtends;
import com.mysema.codegen.model.Types;
import com.mysema.query.types.ConstructorExpression;
import com.mysema.query.types.Expression;
import com.mysema.query.types.Path;
import com.mysema.query.types.PathMetadata;
import com.mysema.query.types.PathMetadataFactory;
import com.mysema.query.types.expr.ComparableExpression;
import com.mysema.query.types.expr.SimpleExpression;
import com.mysema.query.types.path.ArrayPath;
import com.mysema.query.types.path.BooleanPath;
import com.mysema.query.types.path.CollectionPath;
import com.mysema.query.types.path.ComparablePath;
import com.mysema.query.types.path.DatePath;
import com.mysema.query.types.path.DateTimePath;
import com.mysema.query.types.path.EntityPathBase;
import com.mysema.query.types.path.EnumPath;
import com.mysema.query.types.path.ListPath;
import com.mysema.query.types.path.MapPath;
import com.mysema.query.types.path.NumberPath;
import com.mysema.query.types.path.PathInits;
import com.mysema.query.types.path.SetPath;
import com.mysema.query.types.path.SimplePath;
import com.mysema.query.types.path.StringPath;
import com.mysema.query.types.path.TimePath;

/**
 * EntitySerializer is a {@link Serializer} implementation for entity types
 *
 * @author tiwe
 *
 */
public class EntitySerializer implements Serializer{

    private static final Joiner JOINER = Joiner.on("\", \"");

    private static final Parameter PATH_METADATA = new Parameter("metadata", new ClassType(PathMetadata.class, (Type)null));

    private static final Parameter PATH_INITS = new Parameter("inits", new ClassType(PathInits.class));

    private static final ClassType PATH_INITS_TYPE = new ClassType(PathInits.class);

    protected final TypeMappings typeMappings;

    protected final Collection<String> keywords;

    /**
     * Create a new EntitySerializer instance
     *
     * @param mappings
     * @param keywords
     */
    @Inject
    public EntitySerializer(TypeMappings mappings, @Named("keywords") Collection<String> keywords) {
        this.typeMappings = mappings;
        this.keywords = keywords;
    }

    protected void constructors(EntityType model, SerializerConfig config,
            CodeWriter writer) throws IOException {
        String localName = writer.getRawName(model);
        String genericName = writer.getGenericName(true, model);

        boolean hasEntityFields = model.hasEntityFields();
        boolean stringOrBoolean = model.getOriginalCategory() == TypeCategory.STRING
                || model.getOriginalCategory() == TypeCategory.BOOLEAN;
        String thisOrSuper = hasEntityFields ? THIS : SUPER;
        String additionalParams = getAdditionalConstructorParameter(model);
        String classCast = localName.equals(genericName) ? EMPTY : "(Class)";

        // String
        constructorsForVariables(writer, model);

        // Path
        if (!localName.equals(genericName)) {
            writer.suppressWarnings("all");
        }
        Type simpleModel = new SimpleType(model);
        if (model.isFinal()) {
            Type type = new ClassType(Path.class, simpleModel);
            writer.beginConstructor(new Parameter("path", type));
        } else {
            Type type = new ClassType(Path.class, new TypeExtends(simpleModel));
            writer.beginConstructor(new Parameter("path", type));
        }
        if (!hasEntityFields) {
            if (stringOrBoolean) {
                writer.line("super(path.getMetadata());");
            } else {
                writer.line("super(", classCast, "path.getType(), path.getMetadata()" +additionalParams+");");
            }
        } else {
            writer.line("this(", classCast, "path.getType(), path.getMetadata(), path.getMetadata().isRoot() ? INITS : PathInits.DEFAULT);");
        }
        writer.end();

        // PathMetadata
        if (hasEntityFields) {
            writer.beginConstructor(PATH_METADATA);
            writer.line("this(metadata, metadata.isRoot() ? INITS : PathInits.DEFAULT);");
            writer.end();
        } else {
            if (!localName.equals(genericName)) {
                writer.suppressWarnings("all");
            }
            writer.beginConstructor(PATH_METADATA);
            if (stringOrBoolean) {
                writer.line("super(metadata);");
            } else {
                writer.line("super(", classCast, localName, ".class, metadata" + additionalParams + ");");
            }
            writer.end();
        }

        // PathMetadata, PathInits
        if (hasEntityFields) {
            if (!localName.equals(genericName)) {
                writer.suppressWarnings("all");
            }
            writer.beginConstructor(PATH_METADATA, PATH_INITS);
            writer.line(thisOrSuper, "(", classCast, localName, ".class, metadata, inits" + additionalParams+ ");");
            writer.end();
        }

        // Class, PathMetadata, PathInits
        if (hasEntityFields) {
            Type type = new ClassType(Class.class, new TypeExtends(model));
            writer.beginConstructor(new Parameter("type", type), PATH_METADATA, PATH_INITS);
            writer.line("super(type, metadata, inits"+additionalParams+");");
            initEntityFields(writer, config, model);
            writer.end();
        }

    }

    protected String getAdditionalConstructorParameter(EntityType model) {
        return "";
    }

    protected void constructorsForVariables(CodeWriter writer, EntityType model) throws IOException {
        String localName = writer.getRawName(model);
        String genericName = writer.getGenericName(true, model);

        boolean stringOrBoolean = model.getOriginalCategory() == TypeCategory.STRING
                || model.getOriginalCategory() == TypeCategory.BOOLEAN;
        boolean hasEntityFields = model.hasEntityFields();
        String thisOrSuper = hasEntityFields ? THIS : SUPER;
        String additionalParams = hasEntityFields ? "" : getAdditionalConstructorParameter(model);

        if (!localName.equals(genericName)) {
            writer.suppressWarnings("all");
        }
        writer.beginConstructor(new Parameter("variable", Types.STRING));
        if (stringOrBoolean) {
            writer.line(thisOrSuper,"(forVariable(variable)",additionalParams,");");
        } else {
            writer.line(thisOrSuper,"(", localName.equals(genericName) ? EMPTY : "(Class)",
                    localName, ".class, forVariable(variable)", hasEntityFields ? ", INITS" : EMPTY,
                            additionalParams,");");
        }
        writer.end();
    }

    protected void entityAccessor(EntityType model, Property field, CodeWriter writer) throws IOException {
        Type queryType = typeMappings.getPathType(field.getType(), model, false);
        writer.beginPublicMethod(queryType, field.getEscapedName());
        writer.line("if (", field.getEscapedName(), " == null) {");
        writer.line("    ", field.getEscapedName(), " = new ", writer.getRawName(queryType),
                "(forProperty(\"", field.getName(), "\"));");
        writer.line("}");
        writer.line(RETURN, field.getEscapedName(), SEMICOLON);
        writer.end();
    }

    protected void entityField(EntityType model, Property field, SerializerConfig config,
            CodeWriter writer) throws IOException {
        Type queryType = typeMappings.getPathType(field.getType(), model, false);
        if (field.isInherited()) {
            writer.line("// inherited");
        }
        if (config.useEntityAccessors()) {
            writer.protectedField(queryType, field.getEscapedName());
        } else {
            writer.publicFinal(queryType, field.getEscapedName());
        }
    }

    protected boolean hasOwnEntityProperties(EntityType model) {
        if (model.hasEntityFields()) {
            for (Property property : model.getProperties()) {
                if (!property.isInherited() && property.getType().getCategory() == TypeCategory.ENTITY) {
                    return true;
                }
            }
        }
        return false;
    }

    protected void initEntityFields(CodeWriter writer, SerializerConfig config,
            EntityType model) throws IOException {
        Supertype superType = model.getSuperType();
        if (superType != null && superType.getEntityType() == null) {
            throw new IllegalStateException("No entity type for " + superType.getType().getFullName());
        }
        if (superType != null && superType.getEntityType().hasEntityFields()) {
            Type superQueryType = typeMappings.getPathType(superType.getEntityType(), model, false);
            writer.line("this._super = new " + writer.getRawName(superQueryType) + "(type, metadata, inits);");
        }

        for (Property field : model.getProperties()) {
            if (field.getType().getCategory() == TypeCategory.ENTITY) {
                initEntityField(writer, config, model, field);

            } else if (field.isInherited() && superType != null && superType.getEntityType().hasEntityFields()) {
                writer.line("this.", field.getEscapedName(), " = _super.", field.getEscapedName(), SEMICOLON);
            }
        }
    }

    protected void initEntityField(CodeWriter writer, SerializerConfig config, EntityType model,
            Property field) throws IOException {
        Type queryType = typeMappings.getPathType(field.getType(), model, false);
        if (!field.isInherited()) {
            boolean hasEntityFields = field.getType() instanceof EntityType
                    && ((EntityType)field.getType()).hasEntityFields();
            writer.line("this." + field.getEscapedName() + ASSIGN,
                "inits.isInitialized(\""+field.getName()+"\") ? ",
                NEW + writer.getRawName(queryType) + "(forProperty(\"" + field.getName() + "\")",
                hasEntityFields ? (", inits.get(\""+field.getName()+"\")") : EMPTY,
                ") : null;");
        } else if (!config.useEntityAccessors()) {
            writer.line("this.", field.getEscapedName(), ASSIGN, "_super.", field.getEscapedName(), SEMICOLON);
        }
    }

    protected void intro(EntityType model, SerializerConfig config,
            CodeWriter writer) throws IOException {
        introPackage(writer, model);
        introImports(writer, config, model);

        writer.nl();

        introJavadoc(writer, model);
        introClassHeader(writer, model);

        introFactoryMethods(writer, model);
        introInits(writer, model);
        if (config.createDefaultVariable()) {
            introDefaultInstance(writer, model, config.defaultVariableName());
        }
        if (model.getSuperType() != null && model.getSuperType().getEntityType() != null) {
            introSuper(writer, model);
        }
    }

    @SuppressWarnings(UNCHECKED)
    protected void introClassHeader(CodeWriter writer, EntityType model) throws IOException {
        Type queryType = typeMappings.getPathType(model, model, true);

        TypeCategory category = model.getOriginalCategory();
        Class<? extends Path> pathType;

        if (model.getProperties().isEmpty()) {
            switch(category) {
                case COMPARABLE : pathType = ComparablePath.class; break;
                case ENUM: pathType = EnumPath.class; break;
                case DATE: pathType = DatePath.class; break;
                case DATETIME: pathType = DateTimePath.class; break;
                case TIME: pathType = TimePath.class; break;
                case NUMERIC: pathType = NumberPath.class; break;
                case STRING: pathType = StringPath.class; break;
                case BOOLEAN: pathType = BooleanPath.class; break;
                default : pathType = EntityPathBase.class;
            }
        } else {
            pathType = EntityPathBase.class;
        }

        for (Annotation annotation : model.getAnnotations()) {
            writer.annotation(annotation);
        }

        writer.line("@Generated(\"", getClass().getName(), "\")");

        if (category == TypeCategory.BOOLEAN || category == TypeCategory.STRING) {
            writer.beginClass(queryType, new ClassType(pathType));
        } else {
            writer.beginClass(queryType, new ClassType(category, pathType, model));
        }

        // TODO : generate proper serialVersionUID here
        long serialVersionUID = model.getFullName().hashCode();
        writer.privateStaticFinal(Types.LONG_P, "serialVersionUID", String.valueOf(serialVersionUID));
    }

    protected void introDefaultInstance(CodeWriter writer, EntityType model, String defaultName) throws IOException {
        String simpleName = !defaultName.isEmpty() ? defaultName : model.getUncapSimpleName();
        Type queryType = typeMappings.getPathType(model, model, true);
        String alias = simpleName;
        if (keywords.contains(simpleName.toUpperCase())) {
            alias += "1";
        }
        writer.publicStaticFinal(queryType, simpleName, NEW + queryType.getSimpleName() + "(\"" + alias + "\")");

    }

    protected void introFactoryMethods(CodeWriter writer, final EntityType model) throws IOException {
        String localName = writer.getRawName(model);
        String genericName = writer.getGenericName(true, model);

        for (Constructor c : model.getConstructors()) {
            // begin
            if (!localName.equals(genericName)) {
                writer.suppressWarnings(UNCHECKED);
            }
            Type returnType = new ClassType(ConstructorExpression.class, model);
            writer.beginStaticMethod(returnType, "create", c.getParameters(),
                    new Function<Parameter, Parameter>() {
                @Override
                public Parameter apply(Parameter p) {
                    return new Parameter(p.getName(), typeMappings.getExprType(
                            p.getType(), model, false, false, true));
                }
            });

            // body
            // TODO : replace with class reference
            writer.beginLine("return new ConstructorExpression<" + genericName + ">(");
            if (!localName.equals(genericName)) {
                writer.append("(Class)");
            }
            writer.append(localName + DOT_CLASS);
            writer.append(", new Class[]{");
            boolean first = true;
            for (Parameter p : c.getParameters()) {
                if (!first) {
                    writer.append(COMMA);
                }
                if (Types.PRIMITIVES.containsKey(p.getType())) {
                    Type primitive = Types.PRIMITIVES.get(p.getType());
                    writer.append(primitive.getFullName()+DOT_CLASS);
                } else {
                    writer.append(writer.getRawName(p.getType()));
                    writer.append(DOT_CLASS);
                }
                first = false;
            }
            writer.append("}");

            for (Parameter p : c.getParameters()) {
                writer.append(COMMA + p.getName());
            }

            // end
            writer.append(");\n");
            writer.end();
        }
    }

    protected void introImports(CodeWriter writer, SerializerConfig config,
            EntityType model) throws IOException {
        writer.staticimports(PathMetadataFactory.class);

        // import package of query type
        Type queryType = typeMappings.getPathType(model, model, true);
        if (!model.getPackageName().isEmpty()
            && !queryType.getPackageName().equals(model.getPackageName())
            && !queryType.getSimpleName().equals(model.getSimpleName())) {
            String fullName = model.getFullName();
            String packageName = model.getPackageName();
            if (fullName.substring(packageName.length()+1).contains(".")) {
                fullName = fullName.substring(0, fullName.lastIndexOf('.'));
            }
            writer.importClasses(fullName);
        }

        // delegate packages
        introDelegatePackages(writer, model);

        // other packages
        List<Package> packages = Lists.newArrayList();
        packages.add(SimplePath.class.getPackage());
        if (!model.getConstructors().isEmpty()) {
            packages.add(SimpleExpression.class.getPackage());
        }
        if (isImportExprPackage(model)) {
            packages.add(ComparableExpression.class.getPackage());
        }
        writer.imports(packages.toArray(new Package[packages.size()]));

        // other classes
        List<Class<?>> classes = Lists.<Class<?>>newArrayList(PathMetadata.class, Generated.class);
        if (!getUsedClassNames(model).contains("Path")) {
            classes.add(Path.class);
        }
        if (!model.getConstructors().isEmpty()) {
            classes.add(ConstructorExpression.class);
            classes.add(Expression.class);
        }
        boolean inits = false;
        if (model.hasEntityFields() || model.hasInits()) {
            inits = true;
        } else {
            Set<TypeCategory> collections = Sets.newHashSet(TypeCategory.COLLECTION, TypeCategory.LIST, TypeCategory.SET);
            for (Property property : model.getProperties()) {
                if (!property.isInherited() && collections.contains(property.getType().getCategory())) {
                    inits = true;
                    break;
                }
            }
        }
        if (inits) {
            classes.add(PathInits.class);
        }
        writer.imports(classes.toArray(new Class[classes.size()]));
    }

    private Set<String> getUsedClassNames(EntityType model) {
        Set<String> result = Sets.newHashSet();
        result.add(model.getSimpleName());
        for (Property property : model.getProperties()) {
            result.add(property.getType().getSimpleName());
            for (Type type : property.getType().getParameters()) {
                if (type != null) {
                    result.add(type.getSimpleName());
                }
            }
        }
        return result;
    }

    protected boolean isImportExprPackage(EntityType model) {
        if (!model.getConstructors().isEmpty() || !model.getDelegates().isEmpty()) {
            boolean importExprPackage = false;
            for (Constructor c : model.getConstructors()) {
                for (Parameter cp : c.getParameters()) {
                    importExprPackage |= cp.getType().getPackageName()
                            .equals(ComparableExpression.class.getPackage().getName());
                }
            }
            for (Delegate d : model.getDelegates()) {
                for (Parameter dp : d.getParameters()) {
                    importExprPackage |= dp.getType().getPackageName()
                            .equals(ComparableExpression.class.getPackage().getName());
                }
            }
            return importExprPackage;

        } else {
            return false;
        }
    }

    protected void introDelegatePackages(CodeWriter writer, EntityType model) throws IOException {
        Set<String> packages = new HashSet<String>();
        for (Delegate delegate : model.getDelegates()) {
            if (!delegate.getDelegateType().getPackageName().equals(model.getPackageName())) {
                packages.add(delegate.getDelegateType().getPackageName());
            }
        }
        writer.importPackages(packages.toArray(new String[packages.size()]));
    }

    protected void introInits(CodeWriter writer, EntityType model) throws IOException {
        List<String> inits = new ArrayList<String>();
        for (Property property : model.getProperties()) {
            for (String init : property.getInits()) {
                inits.add(property.getEscapedName() + DOT + init);
            }
        }
        if (!inits.isEmpty()) {
            inits.add(0, STAR);
            String initsAsString = QUOTE + JOINER.join(inits) + QUOTE;
            writer.privateStaticFinal(PATH_INITS_TYPE, "INITS", "new PathInits(" + initsAsString + ")");
        } else if (model.hasEntityFields()) {
            writer.privateStaticFinal(PATH_INITS_TYPE, "INITS", "PathInits.DIRECT2");
        }
    }

    protected void introJavadoc(CodeWriter writer, EntityType model) throws IOException {
        Type queryType = typeMappings.getPathType(model, model, true);
        writer.javadoc(queryType.getSimpleName() + " is a Querydsl query type for " +
        model.getSimpleName());
    }

    protected void introPackage(CodeWriter writer, EntityType model) throws IOException {
        Type queryType = typeMappings.getPathType(model, model, false);
        if (!queryType.getPackageName().isEmpty()) {
            writer.packageDecl(queryType.getPackageName());
        }
    }

    protected void introSuper(CodeWriter writer, EntityType model) throws IOException {
        EntityType superType = model.getSuperType().getEntityType();
        Type superQueryType = typeMappings.getPathType(superType, model, false);
        if (!superType.hasEntityFields()) {
            writer.publicFinal(superQueryType, "_super", NEW + writer.getRawName(superQueryType) + "(this)");
        } else {
            writer.publicFinal(superQueryType, "_super");
        }
    }

    protected void listAccessor(EntityType model, Property field, CodeWriter writer) throws IOException {
        String escapedName = field.getEscapedName();
        Type queryType = typeMappings.getPathType(field.getParameter(0), model, false);

        writer.beginPublicMethod(queryType, escapedName, new Parameter("index", Types.INT));
        writer.line(RETURN + escapedName + ".get(index);").end();

        writer.beginPublicMethod(queryType, escapedName, new Parameter("index",
                new ClassType(Expression.class, Types.INTEGER)));
        writer.line(RETURN + escapedName +".get(index);").end();
    }

    protected void mapAccessor(EntityType model, Property field, CodeWriter writer) throws IOException {
        String escapedName = field.getEscapedName();
        Type queryType = typeMappings.getPathType(field.getParameter(1), model, false);

        writer.beginPublicMethod(queryType, escapedName, new Parameter("key", field.getParameter(0)));
        writer.line(RETURN + escapedName + ".get(key);").end();

        writer.beginPublicMethod(queryType, escapedName, new Parameter("key",
                new ClassType(Expression.class, field.getParameter(0))));
        writer.line(RETURN + escapedName + ".get(key);").end();
    }

    private void delegate(final EntityType model, Delegate delegate, SerializerConfig config,
            CodeWriter writer) throws IOException {
        Parameter[] params = delegate.getParameters().toArray(new Parameter[delegate.getParameters().size()]);
        writer.beginPublicMethod(delegate.getReturnType(), delegate.getName(), params);

        // body start
        writer.beginLine(RETURN + delegate.getDelegateType().getSimpleName() + "."+delegate.getName()+"(");
        writer.append("this");
        if (!model.equals(delegate.getDeclaringType())) {
            int counter = 0;
            EntityType type = model;
            while (type != null && !type.equals(delegate.getDeclaringType())) {
                type = type.getSuperType() != null ? type.getSuperType().getEntityType() : null;
                counter++;
            }
            for (int i = 0; i < counter; i++) {
                writer.append("._super");
            }
        }
        for (Parameter parameter : delegate.getParameters()) {
            writer.append(COMMA + parameter.getName());
        }
        writer.append(");\n");

        // body end
        writer.end();
    }

    protected void outro(EntityType model, CodeWriter writer) throws IOException {
        writer.end();
    }

    @Override
    public void serialize(EntityType model, SerializerConfig config,
            CodeWriter writer) throws IOException{
        intro(model, config, writer);

        // properties
        serializeProperties(model, config, writer);

        // constructors
        constructors(model, config, writer);

        // delegates
        for (Delegate delegate : model.getDelegates()) {
            delegate(model, delegate, config, writer);
        }

        // property accessors
        for (Property property : model.getProperties()) {
            TypeCategory category = property.getType().getCategory();
            if (category == TypeCategory.MAP && config.useMapAccessors()) {
                mapAccessor(model, property, writer);
            } else if (category == TypeCategory.LIST && config.useListAccessors()) {
                listAccessor(model, property, writer);
            } else if (category == TypeCategory.ENTITY && config.useEntityAccessors()) {
                entityAccessor(model, property, writer);
            }
        }
        outro(model, writer);
    }

    protected void serialize(EntityType model, Property field, Type type, CodeWriter writer,
            String factoryMethod, String... args) throws IOException {
        Supertype superType = model.getSuperType();
        // construct value
        StringBuilder value = new StringBuilder();
        if (field.isInherited() && superType != null) {
            if (!superType.getEntityType().hasEntityFields()) {
                value.append("_super." + field.getEscapedName());
            }
        } else {
            value.append(factoryMethod + "(\"" + field.getName() + QUOTE);
            for (String arg : args) {
                value.append(COMMA + arg);
            }
            value.append(")");
        }

        // serialize it
        if (field.isInherited()) {
            writer.line("//inherited");
        }
        if (value.length() > 0) {
            writer.publicFinal(type, field.getEscapedName(), value.toString());
        } else {
            writer.publicFinal(type, field.getEscapedName());
        }
    }

    private void customField(EntityType model, Property field, SerializerConfig config,
            CodeWriter writer) throws IOException {
        Type queryType = typeMappings.getPathType(field.getType(), model, false);
        writer.line("// custom");
        if (field.isInherited()) {
            writer.line("// inherited");
            Supertype superType = model.getSuperType();
            if (!superType.getEntityType().hasEntityFields()) {
                writer.publicFinal(queryType, field.getEscapedName(),"_super." + field.getEscapedName());
            } else {
                writer.publicFinal(queryType, field.getEscapedName());
            }
        } else {
            String value = NEW + writer.getRawName(queryType) + "(forProperty(\"" + field.getName() + "\"))";
            writer.publicFinal(queryType, field.getEscapedName(), value);
        }
    }

    // TODO move this to codegen
    private Type wrap(Type type) {
        if (type.equals(Types.BOOLEAN_P)) {
            return Types.BOOLEAN;
        } else if (type.equals(Types.BYTE_P)) {
            return Types.BYTE;
        } else if (type.equals(Types.CHAR)) {
            return Types.CHARACTER;
        } else if (type.equals(Types.DOUBLE_P)) {
            return Types.DOUBLE;
        } else if (type.equals(Types.FLOAT_P)) {
            return Types.FLOAT;
        } else if (type.equals(Types.INT)) {
            return Types.INTEGER;
        } else if (type.equals(Types.LONG_P)) {
            return Types.LONG;
        } else if (type.equals(Types.SHORT_P)) {
            return Types.SHORT;
        } else {
            return type;
        }
    }

    protected void serializeProperties(EntityType model,  SerializerConfig config,
            CodeWriter writer) throws IOException {
        for (Property property : model.getProperties()) {
            // FIXME : the custom types should have the custom type category
            if (typeMappings.isRegistered(property.getType())
                    && property.getType().getCategory() != TypeCategory.CUSTOM
                    && property.getType().getCategory() != TypeCategory.ENTITY) {
                customField(model, property, config, writer);
                continue;
            }

            // strips of "? extends " etc
            Type propertyType = new SimpleType(property.getType(), property.getType().getParameters());
            Type queryType = typeMappings.getPathType(propertyType, model, false);
            Type genericQueryType = null;
            String localRawName = writer.getRawName(property.getType());
            String inits = getInits(property);

            switch(property.getType().getCategory()) {
            case STRING:
                serialize(model, property, queryType, writer, "createString");
                break;

            case BOOLEAN:
                serialize(model, property, queryType, writer, "createBoolean");
                break;

            case SIMPLE:
                serialize(model, property, queryType, writer, "createSimple", localRawName + DOT_CLASS);
                break;

            case COMPARABLE:
                serialize(model, property, queryType, writer, "createComparable", localRawName + DOT_CLASS);
                break;

            case ENUM:
                serialize(model, property, queryType, writer, "createEnum", localRawName + DOT_CLASS);
                break;

            case DATE:
                serialize(model, property, queryType, writer, "createDate", localRawName + DOT_CLASS);
                break;

            case DATETIME:
                serialize(model, property, queryType, writer, "createDateTime", localRawName + DOT_CLASS);
                break;

            case TIME:
                serialize(model, property, queryType, writer, "createTime", localRawName + DOT_CLASS);
                break;

            case NUMERIC:
                serialize(model, property, queryType, writer, "createNumber", localRawName + DOT_CLASS);
                break;

            case CUSTOM:
                customField(model, property, config, writer);
                break;

            case ARRAY:
                serialize(model, property, new ClassType(ArrayPath.class,
                        property.getType(),
                        wrap(property.getType().getComponentType())),
                        writer, "createArray", localRawName + DOT_CLASS);
                break;

            case COLLECTION:
                genericQueryType = typeMappings.getPathType(getRaw(property.getParameter(0)), model, false);
                String genericKey = writer.getGenericName(true, property.getParameter(0));
                localRawName = writer.getRawName(property.getParameter(0));
                queryType = typeMappings.getPathType(property.getParameter(0), model, true);

                serialize(model, property, new ClassType(CollectionPath.class, getRaw(property.getParameter(0)), genericQueryType),
                        writer, "this.<"+genericKey + COMMA + writer.getGenericName(true, genericQueryType) + ">createCollection",
                        localRawName + DOT_CLASS, writer.getRawName(queryType) + DOT_CLASS, inits);
                break;

            case SET:
                genericQueryType = typeMappings.getPathType(getRaw(property.getParameter(0)), model, false);
                genericKey = writer.getGenericName(true, property.getParameter(0));
                localRawName = writer.getRawName(property.getParameter(0));
                queryType = typeMappings.getPathType(property.getParameter(0), model, true);

                serialize(model, property, new ClassType(SetPath.class, getRaw(property.getParameter(0)), genericQueryType),
                        writer, "this.<"+genericKey + COMMA + writer.getGenericName(true, genericQueryType) + ">createSet",
                        localRawName + DOT_CLASS, writer.getRawName(queryType) + DOT_CLASS, inits);
                break;

            case LIST:
                genericQueryType = typeMappings.getPathType(getRaw(property.getParameter(0)), model, false);
                genericKey = writer.getGenericName(true, property.getParameter(0));
                localRawName = writer.getRawName(property.getParameter(0));
                queryType = typeMappings.getPathType(property.getParameter(0), model, true);

                serialize(model, property, new ClassType(ListPath.class, getRaw(property.getParameter(0)), genericQueryType),
                        writer, "this.<"+genericKey + COMMA + writer.getGenericName(true, genericQueryType) + ">createList",
                        localRawName + DOT_CLASS, writer.getRawName(queryType) + DOT_CLASS, inits);
                break;

            case MAP:
                genericKey = writer.getGenericName(true, property.getParameter(0));
                String genericValue = writer.getGenericName(true, property.getParameter(1));
                genericQueryType = typeMappings.getPathType(getRaw(property.getParameter(1)), model, false);
                String keyType = writer.getRawName(property.getParameter(0));
                String valueType = writer.getRawName(property.getParameter(1));
                queryType = typeMappings.getPathType(property.getParameter(1), model, true);

                serialize(model, property, new ClassType(MapPath.class, getRaw(property.getParameter(0)),
                        getRaw(property.getParameter(1)), genericQueryType),
                        writer, "this.<" + genericKey + COMMA + genericValue + COMMA +
                            writer.getGenericName(true, genericQueryType) + ">createMap",
                        keyType+DOT_CLASS, valueType+DOT_CLASS, writer.getRawName(queryType)+DOT_CLASS);
                break;

            case ENTITY:
                entityField(model, property, config, writer);
                break;
            }
        }
    }

    private String getInits(Property property) {
        if (!property.getInits().isEmpty()) {
            return "INITS.get(\"" + property.getName() + "\")";
        } else {
            return "PathInits.DIRECT2";
        }
    }

    private Type getRaw(Type type) {
        if (type instanceof EntityType && type.getPackageName().startsWith("ext.java")) {
            return type;
        } else {
            return new SimpleType(type, type.getParameters());
        }
    }

}
