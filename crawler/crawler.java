import java.io.IOException;
import java.util.HashSet;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.ArrayList;

import java.net.*;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.File;

public class crawler {
	private static final String USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.95 Safari/537.36";
	
	public static void main (String[] args)
		throws IOException, InterruptedException{
		Crawl("https://sfbay.craigslist.org/d/apts-housing-for-rent/search/apa");
	}

	public static void Crawl(String u) throws IOException, InterruptedException{
		System.out.println("Request url: " + u);
		Document doc = Jsoup.connect(u).userAgent(USER_AGENT).timeout(100000).get();
		Elements results = doc.select("li[data-pid] > p");
		if (results.size() == 0){
			System.out.println("No result, Error!");
		}
		System.out.println("num of results = "+ results.size());
		rentingInfo[] ans = new rentingInfo[results.size()];
		for (int i = 0; i< results.size(); i++){
			
			ans[i] = new rentingInfo();
			ans[i].title = results.get(i).select("a").text();
			if (ans[i].title == null||ans[i].title.length() == 0) ans[i].title = "null";

			ans[i].rentPrice = results.get(i).select(".result-meta").select(".result-price").text();
			if (ans[i].rentPrice == null||ans[i].rentPrice.length() == 0) ans[i].rentPrice = "null";

			ans[i].detailURL = results.get(i).select("a").attr("href");
			if (ans[i].detailURL == null||ans[i].detailURL.length() == 0) ans[i].detailURL = "null";
			
			ans[i].hood = results.get(i).select(".result-meta").select(".result-hood").text();		
			if (ans[i].hood == null||ans[i].hood.length() == 0) ans[i].hood = "null";
		}
		try{
			BufferedWriter ansWriter;
			File outputFile = new File("result.txt");
			if (!outputFile.exists())
				outputFile.createNewFile();
			FileWriter fw = new FileWriter(outputFile.getAbsoluteFile());
			ansWriter = new BufferedWriter(fw);
			for (int i = 0 ; i < results.size(); i++){
				ansWriter.write(i + " title: " + ans[i].title + ", price: " + ans[i].rentPrice + ", URL: " + ans[i].detailURL + ", hood: " + ans[i].hood);
				ansWriter.newLine();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

class rentingInfo {
	String title;
	String rentPrice;
	String detailURL;
	String hood;
}
