package jingsai;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class DataInfo2 implements Writable{
	private String urlstring;
	private long url_sum;
	private int mark;
	public DataInfo2(){}
	public DataInfo2(String urlstring, long url_sum)
	{
		this.urlstring = urlstring;
		this.url_sum = url_sum;
	}
	public void write(DataOutput out) throws IOException {
		out.writeUTF(urlstring);
		out.writeLong(url_sum);
	}
	public void readFields(DataInput in) throws IOException {
		this.urlstring = in.readUTF();
		this.url_sum = in.readLong();
	}
	public String toString()
	{
		if (mark == 1) {
			return "";
		}
		else {
		return urlstring + ":" + url_sum;
		}
	}
	public String getUrlstring() {
		return urlstring;
	}
	public void setUrlstring(String urlstring) {
		this.urlstring = urlstring;
	}
	public long getUrl_sum() {
		return url_sum;
	}
	public void setUrl_sum(long url_sum) {
		this.url_sum = url_sum;
	}
	public int getMark() {
		return mark;
	}
	public void setMark(int mark) {
		this.mark = mark;
	}
	
	
}
