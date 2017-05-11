package edu.upenn.cis455.mapreduce.master;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TimeZone;

/**
 * Helper methods
 * @author cis455
 *
 */
public class Helper {
	
	//Three legal time formats
	static  SimpleDateFormat formatting = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z");
	static  SimpleDateFormat altFormatting = new SimpleDateFormat("EEEEEEE, dd-MM-yy HH:mm:ss z");
	static  SimpleDateFormat altFormatting2 = new SimpleDateFormat("EEE MMM dd HH:mm:ss yy");
	
	public Helper() {
		formatting.setTimeZone(TimeZone.getTimeZone("GMT"));
		altFormatting.setTimeZone(TimeZone.getTimeZone("GMT"));
		altFormatting2.setTimeZone(TimeZone.getTimeZone("GMT"));
	}
	
	/**
	 * Check if a string is normal
	 * @param s
	 * @return
	 */
	public static boolean isValid(String s) {
		return (s != null) && (!s.isEmpty());
	}
	
	/**
	 * A container to store the validity of a file descriptor
	 * @author cis455
	 *
	 */
	public static class FileValidity {
		boolean exists, isDirectory, canRead, isFile;
		public FileValidity (boolean exists, boolean isDirectory, boolean canRead, boolean isFile){
			this.exists = exists;
			this.isDirectory = isDirectory;
			this.canRead = canRead;
			this.isFile = isFile;
		}

	}
	
	public static String[] splitIntoList(String s) {
		String[] ret = s.split(",\\s+");
		return ret;
	}
	
	/**
	 * Return the current date
	 * @return
	 */
	public static String getDate() {
		final Date now = new Date();
		final SimpleDateFormat formatting = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z");
		formatting.setTimeZone(TimeZone.getTimeZone("GMT"));
		return formatting.format(now);
	}
	
	/**
	 * Return the last modified date of a file
	 * @param f
	 * @return
	 */
	public static String getFileDate(File f) {
		 long time = f.lastModified();
		 Date d = new Date(time);
		 formatting.setTimeZone(TimeZone.getTimeZone("GMT"));
		 return formatting.format(d);
		  
	}
	
	/**
	 * Return a date in string
	 * @param f
	 * @return
	 */
	public static String getDate(Date d) {
		 formatting.setTimeZone(TimeZone.getTimeZone("GMT"));
		 return formatting.format(d);
		  
	}
	
	/**
	 * Helper method that parses by colon
	 * @param s
	 * @return
	 */
	public static ArrayList<String> parseColon(String s) {
		String[] headerLine = s.split(":");
		ArrayList<String> ret = new ArrayList<String>();
		if (headerLine == null || headerLine.length == 0) {
			ret.add("Empty key");
			ret.add("");
		}
		else if (headerLine.length == 1) {
			ret.add(headerLine[0].trim());
			ret.add("");
		}
		else if (headerLine.length == 2) {
			ret.add(headerLine[0].trim());
			ret.add(headerLine[1].trim());
		} else {
			ret.add(headerLine[0].trim());
			StringBuffer sb = new StringBuffer();
			for (int i = 1; i < headerLine.length; i++) {
				String str = headerLine[i].trim();
				sb.append(str);
				if (i + 1 != headerLine.length) sb.append(":");
			}
			ret.add(sb.toString());
		}
		return ret;
	}
	
	/**
	 * Testing method that prints the content in a map
	 * @param map
	 */
	public static void printMap(HashMap<String, String> map) {
		Iterator<String> itr = map.keySet().iterator();
		while (itr.hasNext()) {
			String key = itr.next();
			String val = map.get(key);
			System.out.println("KEY:" + key + "		VAL:" + val);
		}
	}
	
	/**
	 * Check whether a path takes you out of the root
	 * Basically, if you have fewer ../ than XX/, then it is secure, otherwise not secure
	 * @param fullPath
	 * @return
	 */
	public static boolean isSecurePath(String fullPath) {
		//System.out.println("Checking isPathSecure!");
		//System.out.println(fullPath);
		String[] levels = fullPath.split("/");
		int counter = 0; 
		boolean ret = true;
		for (int i = 0; i < levels.length; i++) {
//			System.out.println(levels[i]);
			if(i == 0 && levels[i].isEmpty()) continue;
			if (levels[i].equals("..")) counter--;
			else counter++;
			if (counter < 0) {
				ret = false;
				break;
			}
		}
		return ret;
//		System.out.println("colon: " + backUp + " nonColon: " + nonColon);
	}
	
	/**
	 * Wrap string inside html tags
	 * @param s
	 * @return
	 */
	public static String wrapInHtmlTags(String s) {
		//System.out.println("Wrap in tags");
		String ret = "<html>";
		ret += " <body>";
		ret += "<h2>Yueyang's HTTP Server</h2>";
		ret += "<h3>";
		ret += s;
		ret += "</h3>";
		ret += " </body>";
		ret += "</html>";
		return ret;
	}
	
	/**
	 * Trimming root directory by adding /
	 * @param s
	 * @return
	 */
	public static String fixRootDirectory(String s) {
		  if (!s.endsWith("/")) s.concat("/");
		  return s;
	}
	
	
	/**
	 * Checking a file's validity and store in a FileValidity object
	 * @param path
	 * @return
	 */
	public static FileValidity pathChecking(String path) {
		//System.out.println("pathChecking now");
		File file = new File(path);
		FileValidity fv = new FileValidity(false, false, false, false);
		if (file.exists()) fv.exists = true;
		if (file.isDirectory()) fv.isDirectory = true;
		if (file.canRead()) fv.canRead = true;
		if (file.isFile()) fv.isFile = true;
		return fv;
		
	}
	
	/**
	 * Check whether if a HTTP method is uppercased
	 * @param s
	 * @return
	 */
	public static boolean isMethodUppercase(String s) {
		//System.out.println("Checking isMethodUpperCase");
		for (int i = 0; i < s.length(); i++) {
			if (Character.isLowerCase(s.charAt(i))) return false;
		}
		return true;
	}
	
	
	/**
	 * Parsing an absolute path
	 * @param url
	 * @return
	 */
	public static String parseAbsolutePath(String url) {
		//System.out.println("Processing absolute path");
		String localUrl = url;
		//System.out.println(url);
		if (url.startsWith("http://")) {			
			//remove http://
			url = url.substring(7, localUrl.length());
			String host = url.split("\\/")[0];
			//System.out.println("HOST IS" + host);
			int len = host.length();
			localUrl = url.substring(len, url.length());  
		}
//		System.out.println("LOCAL: " + localUrl);
		return localUrl;
	}
	
	
	/**
	 * Checking if a path is absolute
	 * @param url
	 * @return
	 */
	public static boolean isAbs(String url) {
		if (url.startsWith("http://")) return true;
		else return false;
		
	}
	
	/**
	 * Check if a file is modified
	 * @param d
	 * @param f
	 * @return
	 */
	public static boolean isFileModified(Date d, File f) {
		long fileLastModified = f.lastModified();
//		System.out.println("last: " + fileLastModified);
		long sinceThisTime = d.getTime();
//		System.out.println("since this time: " + sinceThisTime);
		return sinceThisTime > fileLastModified;
		
	}
	
	
	/**
	 * Convert a string to a Date object
	 * @param s
	 * @return
	 */
	public static Date convertStringToDate(String s) {
		Date d = null;
		try {
			d = Helper.formatting.parse(s);
		} catch (ParseException e) {
			try {
				d = Helper.altFormatting.parse(s);
			} catch (ParseException e1) {
				try {
					Helper.altFormatting2.parse(s);
				} catch (ParseException e2 ) {
					System.out.println("Date cannot be recognized!");
				}

			}
		}
		return d;
		
	}
	
	/**
	 * Send out 500 errors
	 * @param s
	 */
	public static void send500InternalError(Socket s) {
		try {
			String msg = "HTTP/1.1 500 INTERNAL SERVER ERROR";
			msg = wrapInHtmlTags(msg);
			DataOutputStream out = new DataOutputStream(s.getOutputStream());
			String res = "";
			res += "HTTP/1.1 500 INTERNAL SERVER ERROR \r\n";
			res += "Date: ";
			res += getDate();
			res += "\r\n";
			res += "Server: Yueyang Zheng \r\n";
			res += "Content-Type: text/html \r\n";
			res += "Content-Length: ";
			res += String.valueOf(msg.length());
			res += "\r\n";
			res += "Connection: closed \r\n\n";
			res += msg;
			out.write(res.getBytes());
			out.flush();
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static String removeSlash(String s) {
		if (s.endsWith("/")) return s.substring(0, s.length() - 1);
		return s;
	}
	


}
