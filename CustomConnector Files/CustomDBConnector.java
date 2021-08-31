

import java.sql.*;
import java.util.*;
import openconnector.AbstractConnector;
import openconnector.ConnectorConfig;
import openconnector.ConnectorException;
import openconnector.Filter;
import openconnector.Item;
import openconnector.Log;
import openconnector.ObjectAlreadyExistsException;
import openconnector.ObjectNotFoundException;
import openconnector.Result;
import openconnector.Schema;

import org.glassfish.hk2.utilities.reflection.Logger;
public class CustomDBConnector extends AbstractConnector
{
	Logger mylogger = Logger.getLogger();
	
	//Schema Attributes
	public static final String ATTR_EMPLID = "employeeId";
	public static final String ATTR_FIRSTNAME = "firstName";
	public static final String ATTR_LASTNAME = "lastName";
	public static final String ATTR_EMAIL = "email";
	public static final String ATTR_STATUS = "status";
	public static final String ATTR_GROUPS = "roles";
	public static final String ATTR_DESC = "description";
	
	
	//Sailpoint Object Type
	private static Map<String, Map<String,Object>> accounts = new HashMap<String, Map<String, Object>>();
	private static Map<String, Map<String,Object>> groups = new HashMap<String, Map<String, Object>>();

	//Connection Parameters
	public final static String CONFIG_USERNAME = "username";
	public final static String CONFIG_PASSWORD = "password";
	public final static String CONFIG_JDBC_URL = "jdbcurl";
	
	public static final String CONFIG_QUERY_ACCOUNTS = "accountQuery";
	public static final String CONFIG_QUERY_GROUPS = "groupQuery";
	
	Connection con = null;
	PreparedStatement ps = null;
	
	public CustomDBConnector() {
		super();		
	}
	public CustomDBConnector(ConnectorConfig config, Log log) {
		super(config, log);
	}
	
	public List<Feature> getSupportedFeatures(String objectType) {
		System.out.println("Inside getSupportedFeatures" +Arrays.asList(Feature.values()));
		return Arrays.asList(Feature.values());
	}
	
	public void testConnection() throws ConnectorException {
		try
		{
			System.out.println("Inside testConnection");
			String jdbcurl = config.getString(CONFIG_JDBC_URL);
			String username = config.getString(CONFIG_USERNAME);
			String password = config.getString(CONFIG_PASSWORD);
			System.out.println(jdbcurl+ " " + username + " "+password);
			
			Class.forName("com.mysql.jdbc.Driver");
			con = DriverManager.getConnection(jdbcurl, username, password);
		}
		catch(Exception e)
		{
			String error = e.getMessage();
			e.printStackTrace();
			throw new ConnectorException("DB connection failed with error : "+error);
		}
	}
	
	public Connection createDBConnection()
	{
		Connection connection = null;
		try 
		{
			System.out.println("Inside createDBConnection");
			String jdbcurl = config.getString(CONFIG_JDBC_URL);
			String username = config.getString(CONFIG_USERNAME);
			String password = config.getString(CONFIG_PASSWORD);
			System.out.println(jdbcurl+ " " + username + " "+password);
			
			Class.forName("com.mysql.jdbc.Driver");
			connection = DriverManager.getConnection(jdbcurl, username, password);
			
			if(connection != null)
			{
				System.out.println("DB Connection Successful");
				System.out.println("DB Connection Created Successfully");			
			}
		}
		catch(SQLException sqle)
		{
			String error = sqle.getMessage();
			sqle.printStackTrace();
			
			throw new ConnectorException("DB connection failed with error : "+error);
		}
		catch(Exception e)
		{
			String error = e.getMessage();
			e.printStackTrace();
			
			throw new ConnectorException("DB connection failed with error : "+error);
		}
		
		return connection;
	}
	
	public List<String> getSupportedObjectTypes() {
		System.out.println("Inside getSupportedObjectTypes");
		List<String> types = super.getSupportedObjectTypes();
		
		types.add(OBJECT_TYPE_ACCOUNT);
		types.add(OBJECT_TYPE_GROUP);
		return types;
	}
	
	@Override
	public Iterator<Map<String, Object>> iterate(Filter filter) throws ConnectorException, UnsupportedOperationException {
		Iterator<Map<String, Object>> iterator = null;
		try 
		{
			String acctQuery = config.getString(CONFIG_QUERY_ACCOUNTS);
			String grpQuery = config.getString(CONFIG_QUERY_GROUPS);
						
			System.out.println("Inside iterate");
			String iterateQuery="";
			if(OBJECT_TYPE_ACCOUNT.equals(this.objectType))
				iterateQuery = acctQuery;
			
			else
				iterateQuery = grpQuery;
			
			con = createDBConnection();
			ps = con.prepareStatement(iterateQuery);
			ResultSet rs = ps.executeQuery();
			
			while(rs.next())
			{
				Map<String, Object> rowMap = new HashMap<String, Object>();
				
				if(OBJECT_TYPE_ACCOUNT.equals(this.objectType))
				{
					String empId = rs.getString(ATTR_EMPLID);
					String firstName = rs.getString(ATTR_FIRSTNAME);
					String lastName = rs.getString(ATTR_LASTNAME);
					String email = rs.getString(ATTR_EMAIL);
					String status = rs.getString(ATTR_STATUS);
					String roles = rs.getString(ATTR_GROUPS);
					String description = rs.getString(ATTR_DESC);
					
					rowMap.put(ATTR_EMPLID, empId);
					rowMap.put(ATTR_FIRSTNAME, firstName);
					rowMap.put(ATTR_LASTNAME, lastName);
					rowMap.put(ATTR_EMAIL, email);
					rowMap.put(ATTR_STATUS, status);
					rowMap.put(ATTR_GROUPS, roles);
					rowMap.put(ATTR_DESC, description);
					if(status.equals("Inactive"))
					{
						rowMap.put("IIQDisabled", true);
					}
					accounts.put(empId, rowMap);
					System.out.println("Accounts map : "+accounts.toString());
					System.out.println("Accounts Map Size"+accounts.size());
				}
				else
				{
					String roles = rs.getString(ATTR_GROUPS);
					String description = rs.getString(ATTR_DESC);
					
					
					
					rowMap.put(ATTR_GROUPS, roles);
					rowMap.put(ATTR_DESC, description);
					
					groups.put(roles, rowMap);
					System.out.println("Groups map : "+groups.toString());
					System.out.println("groups Map Size"+groups.size());
				}
			}
			con.close();
		}
		catch(Exception e)
		{
			String error = e.getMessage();
			System.out.println("Error Reading Users : "+error);
			e.printStackTrace();
		}
		
		if(OBJECT_TYPE_ACCOUNT.equals(this.objectType))
		{
			iterator = accounts.values().iterator();
		}
		else
		{
			iterator = groups.values().iterator();
		}
		return iterator;
	}
	
	
	@Override
	public Map<String, Object> read(String nativeIdentity)
			throws ConnectorException, ObjectNotFoundException, UnsupportedOperationException {
		// TODO Auto-generated method stub
		return read(nativeIdentity, false);
	}
	
	public Map<String, Object> read(String nativeIdentity, boolean forUpdate)
			throws ConnectorException, ObjectNotFoundException, UnsupportedOperationException {
		if(null == nativeIdentity)
		{
			throw new IllegalArgumentException("nativeIdentitfier is Required");
		}
		
		Map<String, Object> obj = getObjectsMap().get(nativeIdentity);
		
		
		
		return (forUpdate) ? obj : copy(obj);
	}
	
	private Map<String, Map<String, Object>> getObjectsMap() throws ConnectorException
	{
		if(OBJECT_TYPE_ACCOUNT.equals(this.objectType))
		{
			return accounts;
		}
		else
			return groups;
	}
	
	private Map<String, Object> copy(Map<String, Object> obj)
	{
		
		return (null != obj) ? new HashMap<String, Object>(obj) : null;
	}
	
	
	public Schema discoverSchema() throws ConnectorException, UnsupportedOperationException {
		try {
		 con = createDBConnection();
			Schema schema = new Schema();
		System.out.println("Inside discoverSchema");
		
		if(OBJECT_TYPE_ACCOUNT.equals(this.objectType))
		{
			String acctQuery = config.getString(CONFIG_QUERY_ACCOUNTS);
			ps = con.prepareStatement(acctQuery);
			ResultSet results = ps.executeQuery();
			
			ResultSetMetaData metadata = results.getMetaData();
			 
			int columnCount = metadata.getColumnCount();
			 			 
			System.out.println("test_table columns : ");
			 
			 
			for (int i=1; i<=columnCount; i++) {
			 
			  String columnName = metadata.getColumnName(i);
			 
			  schema.addAttribute(columnName);
			 
			}
		}
		else
		{
			String grpQuery = config.getString(CONFIG_QUERY_GROUPS);
			ResultSet results = ps.executeQuery(grpQuery);
			
			ResultSetMetaData metadata = results.getMetaData();
			 
			int columnCount = metadata.getColumnCount();
			 			 
			System.out.println("test_table columns : ");
			 
			 
			for (int i=1; i<=columnCount; i++) {
			 
			  String columnName = metadata.getColumnName(i);
			 
			  schema.addAttribute(columnName);
			 
			}
		}
		}catch(Exception e)
		{
			String error = e.getMessage();
			System.out.println("Error Discovering Schema for Oject Type : "+ this.objectType+" : "+error);
			e.printStackTrace();
		}
		
		return schema;
		
	}
	
	public Result create(String nativeIdentity, List<Item> items)
			throws ConnectorException, ObjectAlreadyExistsException, UnsupportedOperationException {
		System.out.println("Inside create");
		
		String empId = "";
		String firstName = "";
		String lastName = "";
		String email = "";
		String status = "";
		String roles = "";
		String description = "";
		
		Result result = new Result(Result.Status.Committed);
		
		Map<String, String> userAttrs = new HashMap<String, String>();
		System.out.println("Native Identifier : "+nativeIdentity);
		
		if(items != null)
		{
			for(Item item : items)
			{
				System.out.println("Item name : "+item.getName()+ " , Item Value : "+item.getValue());
				
				if(item.getName().equals(ATTR_EMPLID))
				{
					empId = item.getValue().toString();
				}
				if(item.getName().equals(ATTR_FIRSTNAME))
				{
					firstName = item.getValue().toString();
				}
				if(item.getName().equals(ATTR_LASTNAME))
				{
					lastName = item.getValue().toString();
				}
				if(item.getName().equals(ATTR_EMAIL))
				{
					email = item.getValue().toString();
				}
				if(item.getName().equals(ATTR_STATUS))
				{
					status = item.getValue().toString();
				}
				if(item.getName().equals(ATTR_GROUPS))
				{
					roles = item.getValue().toString();
				}
				if(item.getName().equals(ATTR_DESC))
				{
					description = item.getValue().toString();
				}
			}
		}
		
		try
		{
			userAttrs.put(ATTR_EMPLID, empId);
			if(userAttrs.get("ATTR_EMPLID") == null)
			{
				userAttrs.remove("ATTR_EMPLID");
				userAttrs.put(ATTR_EMPLID, nativeIdentity);
			}
			userAttrs.put(ATTR_FIRSTNAME, firstName);
			userAttrs.put(ATTR_LASTNAME, lastName);
			userAttrs.put(ATTR_EMAIL, email);
			userAttrs.put(ATTR_STATUS, status);
			userAttrs.put(ATTR_GROUPS, roles);
			userAttrs.put(ATTR_DESC, description);
			
			System.out.println("User data : "+userAttrs.toString());
			
			String query = "insert into userData values (?, ?, ?, ?, ?, ?, ?);";
			con = createDBConnection();
			
			ps = con.prepareStatement(query);
			ps.setString(1, userAttrs.get(ATTR_EMPLID));

			ps.setString(2, userAttrs.get(ATTR_FIRSTNAME));
			ps.setString(3, userAttrs.get(ATTR_LASTNAME));
			ps.setString(4, userAttrs.get(ATTR_EMAIL));
			ps.setString(5, userAttrs.get(ATTR_STATUS));
			ps.setString(6, userAttrs.get(ATTR_GROUPS));
			ps.setString(7, userAttrs.get(ATTR_DESC));
			System.out.println(ps.toString());
			ps.execute();
			System.out.println("Db Entry Done!!");
			con.close();
			
		}
		catch(SQLException e)
		{
			result= new Result(Result.Status.Failed);
			String msg = "Request Failed Due to SQl Exception : "+ e.getMessage();
			result.add(msg);
			e.printStackTrace();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		return result;
	}
	
	
	public Result disable(String nativeIdentity, Map<String, Object> options)
			throws ConnectorException, ObjectNotFoundException, UnsupportedOperationException {
		System.out.println("Inside Disable");
		Result result = new Result(Result.Status.Committed);
		try
		{
			String query = "Update userData SET status='Inactive' where employeeId='"+nativeIdentity+"'";
			System.out.println("Disable Query is : "+query);
			con = createDBConnection();
			ps = con.prepareStatement(query);
			ps.execute();
			
			con.close();
			System.out.println("DB Connection Closed!");
			System.out.println("Disablig user "+nativeIdentity);
			System.out.println("Exit Disable");
		}
		catch(Exception e)
		{
			String error = e.getMessage();
			System.out.println("Error Disabling User : "+ nativeIdentity+" : "+error);
			e.printStackTrace();
		}
		return result;
	}
	
	
	public Result enable(String nativeIdentity, Map<String, Object> options)
			throws ConnectorException, ObjectNotFoundException, UnsupportedOperationException {
		System.out.println("Inside Enable");
		Result result = new Result(Result.Status.Committed);
		try
		{
			String query = "Update userData SET status='Active' where employeeId='"+nativeIdentity+"'";
			System.out.println("Enable Query is : "+query);
			con = createDBConnection();
			ps = con.prepareStatement(query);
			ps.execute();
			
			con.close();
			System.out.println("DB Connection Closed!");
			System.out.println("Enabling user "+nativeIdentity);
			System.out.println("Exit Enable");
		}
		catch(Exception e)
		{
			String error = e.getMessage();
			System.out.println("Error Enabling User : "+ nativeIdentity+" : "+error);
			e.printStackTrace();
		}
		return result;
	}
	
	public Result delete(String nativeIdentity, Map<String, Object> options)
			throws ConnectorException, ObjectNotFoundException, UnsupportedOperationException {
		System.out.println("Inside Delete");
		Result result = new Result(Result.Status.Committed);
		try
		{
			String query = "DELETE from userData where employeeId='"+nativeIdentity+"'";
			System.out.println("Delete Query is : "+query);
			con = createDBConnection();
			ps = con.prepareStatement(query);
			ps.execute();
			
			con.close();
			System.out.println("DB Connection Closed!");
			System.out.println("Deleting user "+nativeIdentity);
			System.out.println("Exit Delete");
		}
		catch(Exception e)
		{
			String error = e.getMessage();
			System.out.println("Error Deleting User : "+ nativeIdentity+" : "+error);
			e.printStackTrace();
		}
		return result;
	}
	
	
	@Override
	public Result update(String id, List<Item> items) throws ConnectorException, ObjectNotFoundException,
			IllegalArgumentException, UnsupportedOperationException 
	{

			
		return super.update(id, items);
	}
}
