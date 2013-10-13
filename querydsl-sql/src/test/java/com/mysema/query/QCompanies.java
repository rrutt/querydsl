package com.mysema.query;

import static com.mysema.query.types.PathMetadataFactory.forVariable;

import javax.annotation.Generated;

import com.mysema.query.types.Path;
import com.mysema.query.types.PathMetadata;
import com.mysema.query.types.path.NumberPath;
import com.mysema.query.types.path.StringPath;


/**
 * QCompanies is a Querydsl query type for QCompanies
 */
@Generated("com.mysema.query.sql.codegen.MetaDataSerializer")
public class QCompanies extends com.mysema.query.sql.RelationalPathBase<QCompanies> {

    private static final long serialVersionUID = 1808918375;

    public static final QCompanies companies = new QCompanies("COMPANIES");

    public final NumberPath<Long> id = createNumber("ID", Long.class);

    public final StringPath name = createString("NAME");

    public final com.mysema.query.sql.PrimaryKey<QCompanies> constraint5 = createPrimaryKey(id);

    public QCompanies(String variable) {
        super(QCompanies.class, forVariable(variable), "PUBLIC", "COMPANIES");
    }

    public QCompanies(Path<? extends QCompanies> path) {
        super(path.getType(), path.getMetadata(), "PUBLIC", "COMPANIES");
    }

    public QCompanies(PathMetadata<?> metadata) {
        super(QCompanies.class, metadata, "PUBLIC", "COMPANIES");
    }

}
