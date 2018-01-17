package com.dataflowdeveloper.processors.process;

import java.util.Iterator;
import java.util.Map;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonToDDL {

	/**
	 * 
	 * @param tableName
	 * @param json
	 * @return String DDL SQL
	 */
	public String parse(String tableName, String json) {
		JsonFactory factory = new JsonFactory();

		StringBuilder sql = new StringBuilder(256);
		sql.append("CREATE TABLE ").append(tableName).append(" ( ");

		ObjectMapper mapper = new ObjectMapper(factory);
		JsonNode rootNode = null;
		try {
			rootNode = mapper.readTree(json);
		} catch (Exception e) {
			e.printStackTrace();
		}

		Iterator<Map.Entry<String, JsonNode>> fieldsIterator = rootNode.fields();
		while (fieldsIterator.hasNext()) {

			Map.Entry<String, JsonNode> field = fieldsIterator.next();
			System.out.println("Key: " + field.getKey() + "\tValue:" + field.getValue());

			sql.append(field.getKey());

			if (field.getValue().canConvertToInt()) {
				sql.append(" INT, ");
			} else if (field.getValue().canConvertToLong()) {
				sql.append(" LONG, ");
			} else if (field.getValue().asText().contains("/")) {
				sql.append(" DATE, ");
			} else if (field.getValue().asText().contains("-")) {
				sql.append(" DATE, ");
			} else if (field.getValue().asText().length() > 25) {
				sql.append(" VARCHAR( ").append( field.getValue().asText().length() + 25 ) .append("), ");			
			} else {
				sql.append(" VARCHAR(25), ");
			}
		}

		// end table
		sql.deleteCharAt(sql.length() - 2);
		sql.append(" ) ");
		return sql.toString();
	}

	public static void main(String[] args) {

		JsonToDDL ddl = new JsonToDDL();

		String json = "{\"EMP_ID\":3001,\"DURATION_SEC\":288000,\"LOG_DATE\":\"2017-11-07 10:00:00\"}";

		String ddlSQL = ddl.parse("TIME_LOG", json);

		System.out.println("DDL=" + ddlSQL);

		json = " {\"EMP_ID\":4001,\"GENDER\": \"M\",\"DEPT_ID\":4, \"FIRST_NAME\":\"Brett\",\"LAST_NAME\" :\"Lee\"}";
		ddlSQL = ddl.parse("EMPLOYEE", json);

		System.out.println("DDL=" + ddlSQL);
		
		json = "{\"DEPT_ID\":1,\"CODE\": \"FN\",\"NAME\":\"Finance\",\"DESCRIPTION\" :\"Finance Department\",\"ACTIVE\":1}";
		ddlSQL = ddl.parse("DEPARTMENT", json);

		System.out.println("DDL=" + ddlSQL);
	}
}
