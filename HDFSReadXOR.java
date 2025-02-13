import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;



public class HDFSReadXOR {
	public static void main(String[] args) throws Exception{
		
		//Initialize configuration for hdfs
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		//Defines filepath
		String filePath = "/lab1/bigdata";
		Path path = new Path(filePath);
		
		FSDataInputStream inputStream = fs.open(path);
		
		//This is the given offset
		long startOffset = 1000000000L;
		long endOffset = 1000000999L;
		
		inputStream.seek(startOffset);
		
		
		//Computes XOR offset
		byte checksum = 0;
		for(long i= startOffset; i <= endOffset; i++) {
			int byteVal = inputStream.read();
			if(byteVal == -1) {
				System.out.println("Premature end of file");
			}
			checksum ^= (byte) byteVal;
		}
		
		inputStream.close();
		fs.close();
		System.out.println("8-bit XOR checksum: " + (checksum & 0xFF));
		
	}
}
