package jingsai;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class DataInfo4 implements Writable{
	private long date;
	private long url_sum;
	
	public DataInfo4(){}
	
	public DataInfo4(long date, long url_sum)
	{
		this.date = date;
		this.url_sum = url_sum;
	}

	public void write(DataOutput out) throws IOException {
		out.writeLong(date);
		out.writeLong(url_sum);
		
	}

	public void readFields(DataInput in) throws IOException {
		this.date = in.readLong();
		this.url_sum = in.readLong();
		
	}

	public long getDate() {
		return date;
	}

	public void setDate(long date) {
		this.date = date;
	}

	public long getUrl_sum() {
		return url_sum;
	}

	public void setUrl_sum(long url_sum) {
		this.url_sum = url_sum;
	}
	

}
