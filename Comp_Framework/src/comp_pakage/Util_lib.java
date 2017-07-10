/**
 * 
 */
package comp_pakage;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * @author KV
 *
 */
public class Util_lib {

	public static String GetResultset(String sQuery)
	{
		
		String sResultSetAsString = "";
		
		try {
			
		String sUsername = "sa";
		String sPassword = "";
		
		/*
		 * String url = "Provider=MySQLProv;Data Source=mydb;User Id=myUsername;Password=myPassword;";
		 * Class.forName("com.mysql.jdbc.Driver");
		 */
		
		String url = "jdbc:h2:file:~/dasboot";
		Class.forName("org.h2.Driver");
		
		Connection con = DriverManager.getConnection(url,sUsername,sPassword);
		
		PreparedStatement st = con.prepareStatement(sQuery);
		ResultSet rs = st.executeQuery();
		
		ResultSetMetaData metaData = rs.getMetaData();
		int col = metaData.getColumnCount();
		
		StringBuilder sb = new StringBuilder();
		
		while(rs.next())
		{
			for(int i=1;i<=col;i++)
			{
				String value = rs.getString(i);
				sb.append(value+"\t");
			}
			sb.append("\r\n");
		}
		
		sResultSetAsString = sb.toString();
				
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return sResultSetAsString;
			
	}
	
	public static void SaveToFile(String sContent,String sOutputfilePath, String sfileName)
	{
		try
		{
			File file = new File(sOutputfilePath.concat(sfileName));
			file.getParentFile().mkdir();
			BufferedWriter bw = new BufferedWriter(new FileWriter(file.getAbsoluteFile()));
			bw.write(sContent);
			bw.close();	
		}catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	public static String getText(String sFileName,String sStartText,String sEndText)
	{
		
		String sOutputString = "";
		String sCurrentLine;
		
		try
		{
			BufferedReader br = new BufferedReader(new FileReader(sFileName));
			
			outer:
				while((sCurrentLine = br.readLine()) != null)
				{
					if (sCurrentLine.trim().contains(sStartText))
					{
						while ((sCurrentLine = br.readLine()) != null)
						{
							if(sCurrentLine.trim().contains(sEndText))
							{
								break outer;
							}
							sOutputString = sOutputString+sCurrentLine;
						}
					}
				}
		}
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return sOutputString;
		
	}
	
	public static String getLine(String sFileName,String sStartText)
	{
		String sOutputString = "";
		String sCurrentLine;
		
		try
		{
			BufferedReader br = new BufferedReader(new FileReader(sFileName));
			
			outer:
				while((sCurrentLine = br.readLine()) != null)
				{
					if (sCurrentLine.trim().contains(sStartText))
					{
						sOutputString = sCurrentLine;
						break outer;
					}
				}
		}
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return sOutputString;
	}
	
	public static String getTextAfterLastIndex(String sCurrentLine,String slastIndexString)
	{
		String sOutputString = "";
		try
		{

			int intStringLen = sCurrentLine.length();
			int intLastIndex = sCurrentLine.lastIndexOf(slastIndexString);
			
			sOutputString = sCurrentLine.substring(intLastIndex+1, intStringLen);
		}
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return sOutputString;
		
	}
}
