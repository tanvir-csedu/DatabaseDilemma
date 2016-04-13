package com.tigerit.exam;


import static com.tigerit.exam.IO.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;


/**
 * All of your application logic should be placed inside this class.
 * Remember we will load your application from our custom container.
 * You may add private method inside this class but, make sure your
 * application's execution points start from inside run method.
 */
public class Solution implements Runnable {
    @Override
    public void run() {
    	DatabaseOperation dbOp = new DatabaseOperation();
		
		int numberOfTestCases=readLineAsInteger();
		
		for(int testCase=0; testCase<numberOfTestCases; testCase++){
			int numberOFTables=readLineAsInteger();
			
			for(int tableNum=0; tableNum<numberOFTables; tableNum++){
				String tableName=readLine();
				
				String tmpStr = readLine();
				int numOfColumn=Integer.parseInt(tmpStr.split(" ")[0]);
				int numOfRecord=Integer.parseInt(tmpStr.split(" ")[1]);
				
				String[] columnName = new String[numOfColumn];
				int[][] recordsPerColumn = new int[numOfRecord][numOfColumn];
				
				tmpStr = readLine();
				columnName = tmpStr.split(" ");
								
				dbOp.createTable(tableName, columnName);
				
				for(int i=0;i<numOfRecord;i++){
					tmpStr = readLine();
					String[] columnsValue = tmpStr.split(" ");
					for(int j=0;j<columnsValue.length;j++){
						recordsPerColumn[i][j] = Integer.parseInt(columnsValue[j]);
					}
				}
				dbOp.insertRecordByTableName(tableName, recordsPerColumn);
			}
			
			int numberOfQueries=readLineAsInteger();
			String[] queryStr = new String[numberOfQueries];
			String tmpStr="";
			for(int i=0;i<numberOfQueries;i++){
				queryStr[i]="";
				int j=0;
				while(true){
					tmpStr=readLine();
					if(tmpStr.length()>0){
						queryStr[i]+=tmpStr+" ";
						j++;
					}
					if(j>3){
						break;
					}
				}		
			}
			
			System.out.println("Test: "+(testCase+1));
			dbOp.performQuery(queryStr);
		}
    }
}


class DatabaseOperation {
	public void createTable(String tableName, String[] columnNames){
		Connection conn=null;
		Statement stmt =null;
		try{
			conn =getConnection();
			stmt =conn.createStatement();
			
			String tmpStr="";
			for(int i=1;i<columnNames.length;i++){
				tmpStr+="`"+columnNames[i]+"` INT NOT NULL,"; 
        	}
			
			String sql = "DROP TABLE IF EXISTS `"+tableName+"`";
			stmt.executeUpdate(sql);
			
			sql = "CREATE TABLE `"+tableName+"` (`"+columnNames[0]+"` INT(10) NOT NULL,"
							+ tmpStr 
							+" PRIMARY KEY ("+columnNames[0]+"))";
			stmt.executeUpdate(sql);
			
        }catch(Exception e){
			System.out.println("Exception: "+e.toString());
		}finally{
			try{stmt.close();}catch(Exception e){}
			try{conn.close();}catch(Exception e){}
		}
	}

	public void insertRecordByTableName(String tableName, int[][] recordsPerColumn) {
		Connection conn=null;
		Statement stmt =null;
		try{
			conn =getConnection();
			stmt =conn.createStatement();
			
			String sql = "";
			
			for(int i=0;i<recordsPerColumn.length;i++){
				sql="";
				for(int j=0;j<recordsPerColumn[i].length;j++){
					sql+=recordsPerColumn[i][j]+",";
				}
				sql="INSERT INTO `"+tableName+"` VALUES ("+sql.substring(0, sql.length()-1)+")";
				stmt.executeUpdate(sql);
			}
        }catch(Exception e){
			System.out.println("Exception: "+e.toString());
		}finally{
			try{stmt.close();}catch(Exception e){}
			try{conn.close();}catch(Exception e){}
		}
	}
	
	public void dropTable(String tableName) {
		Connection conn=null;
		Statement stmt =null;
		try{
			conn =getConnection();
			stmt =conn.createStatement();
			
			String sql = "DROP TABLE `"+tableName+"`";
			stmt.executeUpdate(sql);
			
        }catch(Exception e){
			System.out.println("Exception: "+e.toString());
		}finally{
			try{stmt.close();}catch(Exception e){}
			try{conn.close();}catch(Exception e){}
		}
	}

	public void performQuery(String[] queryStr) {
		Connection conn=null;
		Statement stmt =null;
		ResultSet rs = null;
		
		try{
			conn =getConnection();
			stmt =conn.createStatement();
			
			for(int i=0;i<queryStr.length;i++){
				rs = stmt.executeQuery(queryStr[i]);
				
				ResultSetMetaData rsmd=rs.getMetaData();
				
				for(int j=1;j<=rsmd.getColumnCount();j++){
					System.out.print(rsmd.getColumnName(j)+" ");
				}
				System.out.println();
				
				while(rs.next()){
					for(int j=1;j<=rsmd.getColumnCount();j++){
						System.out.print(rs.getInt(j)+" ");
					}
					System.out.println();
				}
				rs.close();
				System.out.println();
			}
        }catch(Exception e){
			System.out.println("Exception: "+e.toString());
		}finally{
			try{stmt.close();}catch(Exception e){}
			try{conn.close();}catch(Exception e){}
		}		
	}
	
	public static Connection getConnection(){
		try{
			Class.forName("com.mysql.jdbc.Driver");
			return DriverManager.getConnection("jdbc:mysql:///query_simulator", "root", "");
		}catch(Exception e){
			System.out.println(e.toString());
		}
		return null;
	}
}
