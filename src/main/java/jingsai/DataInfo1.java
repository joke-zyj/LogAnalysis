package jingsai;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class DataInfo1 implements Writable{
	private String ipstring;
	private long ip_sum;
	private int mark;
	public DataInfo1(){}
	
	public DataInfo1(String ipstring, long ip_sum)
	{
		this.ipstring = ipstring;
		this.ip_sum = ip_sum;
	}
	public void write(DataOutput out) throws IOException {
		out.writeUTF(ipstring);
		out.writeLong(ip_sum);
		
	}
	public void readFields(DataInput in) throws IOException {
		this.ipstring = in.readUTF();
		this.ip_sum = in.readLong();
		
	}
	public String toString()
	{
		if (mark == 1) {
			return "";
		}
		else {
		return ipstring + ":" + ip_sum;
		}
	}

	public String getIpstring() {
		return ipstring;
	}

	public void setIpstring(String ipstring) {
		this.ipstring = ipstring;
	}

	public long getIp_sum() {
		return ip_sum;
	}

	public void setIp_sum(long ip_sum) {
		this.ip_sum = ip_sum;
	}

	public int getMark() {
		return mark;
	}

	public void setMark(int mark) {
		this.mark = mark;
	}
	
	
}
