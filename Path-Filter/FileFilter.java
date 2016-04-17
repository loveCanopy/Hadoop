package HDFS;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class FileFilter implements PathFilter{

	
	private String matcher="*";
//	public void setMatcher(String matcher) {
//		this.matcher = matcher;
//	}
	public FileFilter(String matcher) {
		// TODO Auto-generated constructor stub
		this.matcher = matcher;
	}
	
	@Override
	public boolean accept(Path path) {
		// TODO Auto-generated method stub
		String fileName=path.getName();
		if(fileName.endsWith(".temp")||fileName.endsWith(".crc")){
			return false;
		}
		else{
			return accept(path.toString());
		}
		
	}

	public boolean accept(String filePath){
		boolean isMatcherAll=matcher.equals("*");
		boolean isStrIndex=(filePath.indexOf(matcher)>=0);
		if(isMatcherAll||isStrIndex){
			return true;
		}
		try {
			if(filePath.matches(matcher)){
				return true;
			}
		} catch (Exception e) {
			// TODO: handle exception
			System.err.println(FileFilter.class.getName()+"err for"+matcher);
			return true;
		}
		return false;
	}
	
	
	
}
