package com.mysema.query.dynamodb.impl;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.dynamodb.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.ComparisonOperator;
import com.amazonaws.services.dynamodb.model.Condition;
import com.mysema.query.types.Constant;
import com.mysema.query.types.Expression;
import com.mysema.query.types.FactoryExpression;
import com.mysema.query.types.Operation;
import com.mysema.query.types.Operator;
import com.mysema.query.types.Ops;
import com.mysema.query.types.ParamExpression;
import com.mysema.query.types.Path;
import com.mysema.query.types.Predicate;
import com.mysema.query.types.SubQueryExpression;
import com.mysema.query.types.TemplateExpression;
import com.mysema.query.types.Visitor;

public class DynamodbSerializer implements Visitor<Object, DynamoDBScanExpression> {

	public static final DynamodbSerializer DEFAULT = new DynamodbSerializer();

	@Override
	public Object visit(Constant<?> expr, DynamoDBScanExpression context) {
		// TODO Auto-generated method stub
		throw new RuntimeException();
	}

	@Override
	public Object visit(FactoryExpression<?> expr, DynamoDBScanExpression context) {
		// TODO Auto-generated method stub
		throw new RuntimeException();
	}

	@Override
	public Object visit(Operation<?> expr, DynamoDBScanExpression context) {
		// TODO Auto-generated method stub

		Operator<?> op = expr.getOperator();
		Condition condition = new Condition();
		if (op == Ops.EQ) {
			condition.setComparisonOperator(ComparisonOperator.EQ);

			List<AttributeValue> attributeValueList = fromArgs(expr.getArgs(), condition);
			condition.setAttributeValueList(attributeValueList);
			context.addFilterCondition("firstName", condition);
		}

		return condition;
	}

	private List<AttributeValue> fromArgs(List<Expression<?>> expressions) {
		List<AttributeValue> attributeValueList = new ArrayList<AttributeValue>();

		for (Expression<?> expression : expressions) {
			attributeValueList.add((AttributeValue) expression.accept(this, null));
		}

		return attributeValueList;
	}

	@Override
	public Object visit(ParamExpression<?> expr, DynamoDBScanExpression context) {
		// TODO Auto-generated method stub
		throw new RuntimeException();
	}

	@Override
	public Object visit(Path<?> expr, DynamoDBScanExpression context) {
		AnnotatedElement element = expr.getAnnotatedElement();
		Annotation[] annotations = element.getAnnotations();
		for (Annotation annotation : annotations) {
			if(annotation instanceof DynamoDBRangeKey)
		}
		return new AttributeValue().withS("Jaana");
	}

	@Override
	public Object visit(SubQueryExpression<?> expr, DynamoDBScanExpression context) {
		// TODO Auto-generated method stub
		throw new RuntimeException();
	}

	@Override
	public Object visit(TemplateExpression<?> expr, DynamoDBScanExpression context) {
		// TODO Auto-generated method stub
		throw new RuntimeException();
	}

	public DynamoDBScanExpression handle(Predicate predicate) {
		DynamoDBScanExpression scanExpression = new DynamoDBScanExpression();
		predicate.accept(this, scanExpression);
		return scanExpression;
	}

}
