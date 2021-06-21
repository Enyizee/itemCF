package com.Music.Enyizee;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
public class demo {
	public static void main(String[] args){
		Connection con = HiveJDBC.getConnection();
		Statement stmt;
		try {
			stmt = con.createStatement();
			String sql ="select * from student";
			ResultSet res = stmt.executeQuery(sql);
	        while(res.next()){
	            System.out.println(res.getString(1)+"\t"+res.getString(2)+"\t"+res.getString(3)+"\t"+res.getString(4));
	        }
		} catch (SQLException e) {
			System.out.println("连接Hive的信息有问题");
			e.printStackTrace();
		}  
   }
}
