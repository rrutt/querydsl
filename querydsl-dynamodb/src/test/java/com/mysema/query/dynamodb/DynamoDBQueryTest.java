package com.mysema.query.dynamodb;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.michelboudreau.alternator.AlternatorDB;
import com.michelboudreau.alternatorv2.AlternatorDBClientV2;
import com.mysema.query.dynamodb.domain.QUser;
import com.mysema.query.dynamodb.domain.User;
import com.mysema.query.dynamodb.domain.User.Gender;
import com.mysema.query.types.Predicate;

public class DynamoDBQueryTest {

    private final QUser user = QUser.user;

    static User u1, u2, u3, u4;

    private static DynamoDBMapper mapper;

    private static AlternatorDBClientV2 client;

    private static AlternatorDB db;

    @BeforeClass
    public static void setUp() throws Exception {
        client = new AlternatorDBClientV2();
        mapper = new DynamoDBMapper(client, new DynamoDBMapperConfig(
                DynamoDBMapperConfig.SaveBehavior.CLOBBER,
                DynamoDBMapperConfig.ConsistentReads.CONSISTENT, null));
        db = new AlternatorDB();
        db.start();

        fillTable();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        db.stop();
    }

    public static void fillTable() throws UnknownHostException {
        try {
            client.deleteTable(new DeleteTableRequest("User"));
        } catch (ResourceNotFoundException e) {
            // it happens
        }

        ProvisionedThroughput provisionedThroughput = new ProvisionedThroughput();
        provisionedThroughput.setReadCapacityUnits(1L);
        provisionedThroughput.setWriteCapacityUnits(1L);

        CreateTableRequest createTableRequest = new CreateTableRequest()
                .withTableName("User")
                .withKeySchema(
                        new KeySchemaElement().withAttributeName("id").withKeyType(KeyType.HASH))
                .withProvisionedThroughput(provisionedThroughput)
                .withAttributeDefinitions(
                        new AttributeDefinition().withAttributeName("id").withAttributeType(
                                ScalarAttributeType.S));
        client.createTable(createTableRequest);

        u1 = addUser("Jaakko", "Jantunen", 20, Gender.MALE, null);
        u2 = addUser("Jaakki", "Jantunen", 30, Gender.FEMALE, "One detail");
        u3 = addUser("Jaana", "Aakkonen", 40, Gender.MALE, "No details");
        u4 = addUser("Jaana", "BeekkoNen", 50, Gender.FEMALE, null);
    }

    private static User addUser(String first, String last, int age, Gender gender, String details) {
        User user = new User(first, last, age, new Date());
        user.setGender(gender);
        user.setDetails(details);
        mapper.save(user);
        System.out.println(user.getId());
        return user;
    }

    @Test
    public void learnIt() {
        Condition condition = new Condition();
        List<AttributeValue> attributeValueList = new ArrayList<AttributeValue>();
        attributeValueList.add(new AttributeValue().withS("Jaak"));
        condition.setAttributeValueList(attributeValueList);
        condition.setComparisonOperator(ComparisonOperator.BEGINS_WITH);

        DynamoDBScanExpression scanExpression = new DynamoDBScanExpression();
        scanExpression.addFilterCondition("firstName", condition);
        List<User> result = mapper.scan(User.class, scanExpression);
        result = new ArrayList<User>(result);
        assertThat(result, containsInAnyOrder(u1, u2));
    }

    @Test
    public void eq() {
        User u = where(user.firstName.eq("Jaakko")).uniqueResult();
        assertThat(u, equalTo(u1));
    }

    @Test
    public void notEq() {
        List<User> result = where(user.lastName.ne("Jantunen")).list();
        assertThat(result, containsInAnyOrder(u3, u4));
    }

    @Test
    public void beginsWith() {
        List<User> result = where(user.firstName.startsWith("Jaak")).list();
        assertThat(result, containsInAnyOrder(u1, u2));
    }

    @Test
    public void between() {
        List<User> result = where(user.age.between(29, 41)).list();
        assertThat(result, containsInAnyOrder(u3, u2));
    }

    @Test
    public void contains() {
        List<User> result = where(user.lastName.contains("ekko")).list();
        assertThat(result, containsInAnyOrder(u4));
    }

    @Test
    public void notContains() {
        List<User> result = where(user.lastName.contains("nen").not()).list();
        assertThat(result, containsInAnyOrder(u4));
    }

    @Test
    public void greaterOrEquals() {
        List<User> result = where(user.age.goe(40)).list();
        assertThat(result, containsInAnyOrder(u3, u4));
    }

    @Test
    public void greaterThen() {
        List<User> result = where(user.age.gt(20)).list();
        assertThat(result, containsInAnyOrder(u2, u3, u4));
    }

    @Test
    public void lowerOrEquals() {
        List<User> result = where(user.age.loe(20)).list();
        assertThat(result, containsInAnyOrder(u1));
    }

    @Test
    public void lowerThen() {
        List<User> result = where(user.age.lt(40)).list();
        assertThat(result, containsInAnyOrder(u1, u2));
    }

    @Test
    public void in() {
        List<User> result = where(user.firstName.in("Jaakki", "Jaakko")).list();
        assertThat(result, containsInAnyOrder(u1, u2));
    }

    @Test
    public void isNull() {
        List<User> result = where(user.details.isNull()).list();
        assertThat(result, containsInAnyOrder(u1, u4));
    }

    @Test
    public void isNotNull() {
        List<User> result = where(user.details.isNotNull()).list();
        assertThat(result, containsInAnyOrder(u2, u3));
    }

    private DynamoDBQuery<User> query() {
        return new DynamoDBQuery<User>(client, user);
    }

    private DynamoDBQuery<User> where(Predicate... e) {
        return query().where(e);
    }

}
