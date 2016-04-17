package HDFS;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

class RegexExcludePathFilter implements PathFilter{
    private final String regex;
    public RegexExcludePathFilter(String regex) {
        this.regex = regex;
    }
    @Override
    public boolean accept(Path path) {
        return !path.toString().matches(regex);
    }
	
    
}