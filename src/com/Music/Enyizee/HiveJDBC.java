package com.Music.Enyizee;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
public class HiveJDBC {
	private static final String  drivername ="org.apache.hive.jdbc.HiveDriver";
    private static final String URL = "jdbc:hive2://192.168.126.1:10000/default";
    private static final String USER = "root";
    private static final String PASSWORD = "YangHao2419@";
    private static Connection conn = null;
    public static Connection getConnection(){
        try {
            //1.加载驱动程序
            Class.forName(drivername);
            //2. 获得数据库连接
            conn = DriverManager.getConnection(URL, USER, PASSWORD);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }
}
