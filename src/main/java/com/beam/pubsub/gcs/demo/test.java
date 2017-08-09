package com.beam.pubsub.gcs.demo;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

public class test {

	public static void main(String[] args) {
		DateTime time = new DateTime();
		
		System.out.println(time.toString().replaceAll("[:.+]", "-"));
		
		DateTimeFormatter fmt = ISODateTimeFormat.dateTime();
		 String str = fmt.print(time);
		 
//		 System.out.println(str);
	}
}
