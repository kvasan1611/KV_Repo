/**
 * 
 */
package comp_pakage;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.jruby.embed.PathType;
import org.jruby.embed.ScriptingContainer;
import org.h2.*;

import com.relevantcodes.extentreports.*;

/**
 * @author KV
 *
 */
public class Main_driver {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
	try
	{

		//ExtentReports extent = new ExtentReports("./report/test.html", true);
		
		// creates a toggle for the given test, adds all log events under it    
               
        File folder = new File("./ctrlfile/");
		File [] listOfFiles = folder.listFiles();
		
		for (int i=0; i < listOfFiles.length ; i++)
		{
		
			if (listOfFiles[i].isFile())
			{
				
				String sCtrlFilePath = listOfFiles[i].getAbsolutePath();
				String sFileName = listOfFiles[i].getName();
				String sFilePath = "./ctrlfile/";
				String slogFileName = sFileName.replace(".ctrl",".txt");

				//ExtentTest test = extent.startTest("Test No."+ i, "Comparison Results");
				
				String sSourceQuery = Util_lib.getText(sCtrlFilePath,"source_sql:","target_sql:");
				//String sTargetQuery = Util_lib.getText(sCtrlFilePath,"target_sql:","source_sql:");
				
				System.out.println(sSourceQuery);
				
				String sSrcFileLoc = Util_lib.getLine(sCtrlFilePath,"source_file");
				String sTgtFileLoc = Util_lib.getLine(sCtrlFilePath,"target_file");
				String sSrcFileName = Util_lib.getTextAfterLastIndex(sSrcFileLoc,"/");
				String sTgtFileName = Util_lib.getTextAfterLastIndex(sTgtFileLoc,"/");
				
				String strSrcResultSet = Util_lib.GetResultset(sSourceQuery);
				//String strTgtResultSet = Util_lib.GetResultset(sTargetQuery);
				System.out.println(strSrcResultSet);
				
				Util_lib.SaveToFile(strSrcResultSet,"./data/",sSrcFileName);
				//Util_lib.SaveToFile(strTgtResultSet,"C:\\control\\data\\",sTgtFileName);
				
				//Execute Ruby Script
				/*
				Process p = Runtime.getRuntime().exec(new String[]{"C:\\cygwin\\\bin\\bash.exe"
						,"--login","-i","-c"
						,"./simple_mapping-manual.rb "+sTgtFileName+" >"+slogFileName});
				
				p.waitFor();
				*/
				
				ScriptingContainer container = new ScriptingContainer();
				String[] sCtrl = new String[] {sFileName};
				
				container.setArgv(sCtrl);
				String slog = container.runScriptlet(PathType.ABSOLUTE, "./rubyscrip/simple_mapping_v1.rb").toString();		
				
				Util_lib.SaveToFile(slog,"./log/", slogFileName);
				
				//Analyze log File
				String sLogFile = "./log/"+slogFileName;
				BufferedReader br = new BufferedReader(new FileReader(sLogFile));
				
				String slogCurrentLine;
				Boolean sFailFlag = false;
				int intCntMismatch = 0;
				int intCntMissing = 0;
				String strSrcRecCount ="";
				String strTgtRecCount ="";
				
				while ((slogCurrentLine = br.readLine()) != null)
				{
					if (slogCurrentLine.trim().contains("Failed mapping source record"))
					{
						sFailFlag = true;
						intCntMismatch = intCntMismatch+1; 
					}
					else if (slogCurrentLine.trim().contains("Ran out of target records for source record"))
					{
						sFailFlag = true;
						intCntMissing = intCntMissing+1;
					}
					else if (slogCurrentLine.trim().contains("Summary"))
					{
						while((slogCurrentLine = br.readLine()) != null)
						{
							if(slogCurrentLine.trim().startsWith("======="))
							{
								while((slogCurrentLine = br.readLine()) != null)
								{
									if (slogCurrentLine.trim().contains("source records."))
									{
										int ibeginIndex = slogCurrentLine.indexOf("with ");
										int iendIndex = slogCurrentLine.indexOf(" source");
										strSrcRecCount = slogCurrentLine.substring(ibeginIndex+5, iendIndex).trim();
										System.out.println("Source records count :"+strSrcRecCount);
									}
									else if (slogCurrentLine.trim().contains("target records."))
									{
										int ibeginIndex = slogCurrentLine.indexOf("with ");
										int iendIndex = slogCurrentLine.indexOf(" target");
										strTgtRecCount = slogCurrentLine.substring(ibeginIndex+5, iendIndex).trim();
										System.out.println("Target records count :"+strTgtRecCount);
									}
									else if (slogCurrentLine.trim().contains("target records that could not be found in source"))
									{
										System.out.println("Some Target records missing in source");
									}
								}
							}
						}
					}
				}
				
				br.close();
				
				if(strSrcRecCount.contentEquals(strTgtRecCount))
					System.out.println("Count Match");
				else
				{
					System.out.println("Count MisMatch");
					sFailFlag = true;
				}
				
				if(sFailFlag)
				{
					System.out.println("Observed"+intCntMismatch+" Mapping Mismatches in data");
					//test.log(LogStatus.FAIL, "Data Mismatch: "+intCntMismatch+" Log File: <a herf=\""+sLogFile+"\">Click here</a>");
					intCntMismatch = 0;
					sFailFlag = false;
				}
				else
				{
					//test.log(LogStatus.PASS, "Test Run Success Log File: <a herf=\""+sLogFile+"\">Click here</a>");
				}
				
				//extent.endTest(test);
				//extent.flush();
			}
			
		}
	}
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
