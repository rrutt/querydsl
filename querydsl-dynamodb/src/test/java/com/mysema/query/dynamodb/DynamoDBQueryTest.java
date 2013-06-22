package com.mysema.query.dynamodb;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.*;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.dynamodb.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodb.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodb.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodb.datamodeling.PaginatedScanList;
import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.ComparisonOperator;
import com.amazonaws.services.dynamodb.model.Condition;
import com.amazonaws.services.dynamodb.model.CreateTableRequest;
import com.amazonaws.services.dynamodb.model.DeleteTableRequest;
import com.amazonaws.services.dynamodb.model.KeySchema;
import com.amazonaws.services.dynamodb.model.KeySchemaElement;
import com.amazonaws.services.dynamodb.model.ProvisionedThroughput;
import com.michelboudreau.alternator.AlternatorDB;
import com.michelboudreau.alternator.AlternatorDBClient;
import com.mysema.query.dynamodb.domain.QUser;
import com.mysema.query.dynamodb.domain.User;
import com.mysema.query.dynamodb.domain.User.Gender;
import com.mysema.query.types.Predicate;

public class DynamoDBQueryTest {

	 private final QUser user = QUser.user;

	User u1, u2, u3, u4;

	private AlternatorDBClient client;
	private DynamoDBMapper mapper;
	private AlternatorDB db;

	@Before
	public void setUp() throws Exception {
		this.client = new AlternatorDBClient();
		this.mapper = new DynamoDBMapper(this.client, new DynamoDBMapperConfig(DynamoDBMapperConfig.SaveBehavior.CLOBBER, DynamoDBMapperConfig.ConsistentReads.CONSISTENT, null));
		this.db = new AlternatorDB().start();
	}

	@After
	public void tearDown() throws Exception {
		this.db.stop();
	}

	@Before
	public void before() throws UnknownHostException {
		try {
			this.client.deleteTable(new DeleteTableRequest("User"));
		} catch (com.amazonaws.services.dynamodb.model.ResourceNotFoundException e) {
			// it happens
		}

		ProvisionedThroughput provisionedThroughput = new ProvisionedThroughput();
		provisionedThroughput.setReadCapacityUnits(10L);
		provisionedThroughput.setWriteCapacityUnits(10L);

		CreateTableRequest createTableRequest = new CreateTableRequest().withTableName("User").withKeySchema(new KeySchema(new KeySchemaElement().withAttributeName("id").withAttributeType("S"))).withProvisionedThroughput(provisionedThroughput);
		client.createTable(createTableRequest);

		u1 = addUser("Jaakko", "Jantunen", 20);
		u2 = addUser("Jaakki", "Jantunen", 30);
		u3 = addUser("Jaana", "Aakkonen", 40);
		u4 = addUser("Jaana", "BeekkoNen", 50);
	}

	private User addUser(String first, String last, int age) {
		User user = new User(first, last, age, new Date());
		user.setGender(Gender.MALE);
		mapper.save(user);
		System.out.println(user.getId());
		return user;
	}

	@Test
	public void learnIt() {
		Condition condition = new Condition();
		List<AttributeValue> attributeValueList = new ArrayList<AttributeValue>();
		attributeValueList.add(new AttributeValue().withS("Jaana"));
		condition.setAttributeValueList(attributeValueList);
		condition.setComparisonOperator(ComparisonOperator.EQ);

		DynamoDBScanExpression scanExpression = new DynamoDBScanExpression();
		scanExpression.addFilterCondition("firstName", condition);
		PaginatedScanList<User> result = mapper.scan(User.class, scanExpression);
		System.out.println(result.get(0));
	}

	@Test
	public void List_Keys() {
        User u = where(user.firstName.eq("Jaakko")).list().get(0);
        assertEquals("Jaakko", u.getFirstName());
        assertNull(u.getLastName());
	}

	private DynamoDBQuery<User> query() {
		return new DynamoDBQuery<User>(client, user);
	}

	private DynamoDBQuery<User> where(Predicate... e) {
		return query().where(e);
	}

}
