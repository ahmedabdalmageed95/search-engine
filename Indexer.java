/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package indexer;

/**
 *
 * @author Ahmed Abd El Magid
 */
import static indexer.Indexer.remove_stopwords;
import static indexer.index.select;
import static indexer.index.getconnection;
import static indexer.index.getfreq;
import static indexer.index.geturlsdist;
import static indexer.index.insertlinks;
import static indexer.index.selectx;
import static indexer.index.selector;
import static indexer.index.selectand;
import static indexer.index.selectcnt;
import static indexer.index.selectcnturl;
import org.jsoup.nodes.Document;
import org.jsoup.Jsoup;
import org.jsoup.select.Elements;
import org.tartarus.snowball.ext.englishStemmer;
import java.sql.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Scanner;
import java.util.*;
import java.io.*;
import java.nio.*;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.lang.NoClassDefFoundError;
import java.lang.Exception;
import java.sql.ResultSet;
import static java.lang.Math.*;
import static java.time.Clock.system;
import java.util.logging.Level;
import java.util.logging.Logger;
import static java.lang.Math.log;
import org.jsoup.nodes.Element;
class index implements Runnable {

    public static Connection getconnection() throws Exception {
        try {
            
            String driver = "com.mysql.jdbc.Driver";
            String url = "jdbc:mysql://localhost:3306/search_engine";
            String username = "root";
            String pw = "bronze123";
            Class.forName(driver);
            Connection conn = DriverManager.getConnection(url, username, pw);
            System.out.println("cnnctd");
            return conn;
        } catch (Exception e) {
            System.out.println(e);
        }
        return null;
    }

    public static void insert(String imp, String word, String url, int freq,int count,Connection c ) throws Exception {
        try {

            if (imp == "title") {
                PreparedStatement update = c.prepareStatement("insert into search_eng (Importance,word,url,priority,frequency,size) values ('" + imp + "','" + word + "','" + url + "',1,"+freq+count+");");
                update.executeUpdate();
                
            }
            if (imp == "header") {
                PreparedStatement update = c.prepareStatement("insert into search_eng (Importance,word,url,priority,frequency,size) values ('" + imp + "','" + word + "','" + url + "',2,"+freq+count+ ");");
                update.executeUpdate();
                
            }
            if (imp == "body") {
                PreparedStatement update = c.prepareStatement("insert into search_eng (Importance,word,url,priority,frequency,size) values ('" + imp + "','" + word + "','" + url + "',3,"+freq+count+ ");");
                update.executeUpdate();
               
            }
        } catch (Exception e) {
            System.out.println(e);
        } finally {
            System.out.println("inserted");
            
        }
    }
    
     public static void insertlinks( String url, int size,Connection c ) throws Exception {
        try {

            
                PreparedStatement update = c.prepareStatement("insert into links (url,size) values ('" + url + "'," +size+");");
                update.executeUpdate();
                
          
        } catch (Exception e) {
            System.out.println(e);
        } 
    }
     
        public static void inserthlinks( String url, Connection c ) throws Exception {
        try {

            
                PreparedStatement update = c.prepareStatement("insert into hyper (hlinks) values ('" + url +"');");
                update.executeUpdate();
                
          
        } catch (Exception e) {
            System.out.println(e);
        } 
    }
     
     
     
/////////////////////////////////////////////////////////////////ahmed urlssssssssssssssssssss
    public static ArrayList<String> selector(String []word,Connection c) throws Exception {
        try { // selecting urls containig any words of related phrase
            String s="" ;
            if (word.length==1)
            {
            s="select url from search_eng where word='"+word[0]+"'";
            }
            
            else
            {
            for (int i=0;i<word.length-1;i++)
            {
            s=s+"select  url from search_eng where word= '"+word[i]+"' union ";
            }
            
            s=s+"select  url from search_eng where word= '"+word[word.length-1]+"';";
            }
            PreparedStatement update = c.prepareStatement(s);
            ResultSet r = update.executeQuery();
            ArrayList<String> arr3 = new ArrayList<String>();
            while (r.next()) {
                arr3.add(r.getString("url"));
            }
            return arr3;
            
        } catch (Exception e) {
            System.out.println(e);
        }
       
        return null;

    }
    
    ///////////////////////////////////////////////////////////// ahmed urlsssssssssssssssssss
    public static ArrayList<String> selectand(String []word,Connection c) throws Exception {
        try { //select urls containing all words
            String s="";
             if (word.length==1)
            {
            s="select distinct url from search_eng where word='"+word[0]+"'";
            }
             else
             {
             s="select distinct url from search_eng where " ;
            
            for (int i=0;i<word.length-1;i++)
            {
            s=s+"url in ( select url from search_eng where word ='"+word[i]+"') and ";
            }
            
            s=s+"url in (select url from search_eng where word = '"+word[word.length-1]+"');";
            
            PreparedStatement update = c.prepareStatement(s);
            ResultSet r = update.executeQuery();
            ArrayList<String> arr3 = new ArrayList<String>();
            while (r.next()) {
                arr3.add(r.getString("url"));
            }
            return arr3;
             }
        } catch (Exception e) {
            System.out.println(e);
        }
       
        return null;

    }
    
    //////////////////////////////////////////////////////////////////ahmed urlssssssssssssss
    public static ArrayList<String> select(String []word,Connection c) throws Exception {
           
        try {//sort urls according to words
            String tmp="";
            int ctr=0;
            ArrayList<String> arr = new ArrayList<String>();
            arr=selector(word,c);
            ArrayList<String> arr2 = new ArrayList<String>();
            arr2=selectand(word,c);
            for (int i=0;i<arr2.size();i++)
                 for (int j=0;j<arr.size();j++)
            {
            if (arr2.get(i)==arr.get(j))
            {
            tmp=arr.get(j);
            arr.remove(j);
            arr.add(j, arr.get(ctr));
            arr.remove(ctr);
            arr.add(ctr, tmp);
            ctr++;
            }
            }
            return arr;
    }
           catch (Exception e) {
            System.out.println(e);
        }
       
        return null;
    }
    
    
    
    
     public static void update(int x,Connection c) throws Exception {
        try {
            
            PreparedStatement update = c.prepareStatement("update x set y="+x+" where count='last_indexed';");
            update.executeUpdate();
            //System.out.println("selected");
           
        } catch (Exception e) {
            System.out.println(e);
        }
       

    }
     
      public static ArrayList<Integer> selectx(Connection c) throws Exception {
        try {

            PreparedStatement update = c.prepareStatement("select y from x where count='last_indexed';");
            ResultSet r = update.executeQuery();
            ArrayList<Integer> arr = new ArrayList<Integer>();
            //ArrayList<String> arr2 = new ArrayList<String>();
           // ArrayList<String> arr3 = new ArrayList<String>();
            //ArrayList<Integer> arr4 = new ArrayList<Integer>();
            while (r.next()) {
                //System.out.println(r.getString("Importance"));
                //arr.add(r.getString("Importance"));
                //System.out.println(r.getString("word"));
                //arr2.add(r.getString("word"));
              //  System.out.println(r.getString("url"));
                arr.add(r.getInt("y"));
               
            }
            
            return arr;
            //System.out.println("selected");
 
        } catch (Exception e) {
            System.out.println(e);
        }
       return null;

    }
      
      //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
       public static ArrayList<Integer> selectcnt(Connection c) throws Exception {
        try {//total no of urls in db

            PreparedStatement update = c.prepareStatement("select count(distinct(url)) from search_eng;");
            ResultSet r = update.executeQuery();
            ArrayList<Integer> arr = new ArrayList<Integer>();
        
            while (r.next()) {
                
                arr.add(r.getInt("count(distinct(url))"));
               
            }
            
            return arr;
            //System.out.println("selected");
 
        } catch (Exception e) {
            System.out.println(e);
        }
       return null;

    }
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////       
  public static ArrayList<Integer> getfreq(Connection c, String url) throws Exception
  { //no of words
      try {
    String s="select frequency from search_eng where  url= '"+url +"';" ;
     ArrayList<Integer> arr = new ArrayList<Integer>();
     PreparedStatement update = c.prepareStatement(s);
               ResultSet r = update.executeQuery();
            while (r.next()) {
                
                arr.add(r.getInt("frequency"));
               
            }
            
            return arr;}
      catch (Exception e) {
            System.out.println(e);
        }
       return null;

  }
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public static ArrayList<String> geturls(Connection c) throws Exception {
        try {//total no of urls in db

            PreparedStatement update = c.prepareStatement("select url from search_eng;");
            ResultSet r = update.executeQuery();
            ArrayList<String> arr = new ArrayList<String>();
        
            while (r.next()) {
                
                arr.add(r.getString("url"));
               
            }
            
            return arr;
            //System.out.println("selected");
 
        } catch (Exception e) {
            System.out.println(e);
        }
       return null;

    }
    
     public static ArrayList<String> geturlsdist(Connection c) throws Exception {
        try {//total no of urls in db

            PreparedStatement update = c.prepareStatement("select distinct url from search_eng;");
            ResultSet r = update.executeQuery();
            ArrayList<String> arr = new ArrayList<String>();
        
            while (r.next()) {
                
                arr.add(r.getString("url"));
               
            }
            
            return arr;
            //System.out.println("selected");
 
        } catch (Exception e) {
            System.out.println(e);
        }
       return null;

    }
  ////////////////////////////////////////////////////////////////////////////////////////////////
  /*public static ArrayList<Integer> getsize(Connection c, String url,String []word) throws Exception
  {
      try { //size of doc
    String s="select size from search_eng where  url= '"+url +"' and  " ;
     for (int i=0;i<word.length-1;i++)
            {
            s=s+"size in ( select size from search_eng where word ='"+word[i]+"') or ";
            }
            
            s=s+"size in (select size from search_eng where word = '"+word[word.length-1]+"');";
     ArrayList<Integer> arr = new ArrayList<Integer>();
     PreparedStatement update = c.prepareStatement(s);
               ResultSet r = update.executeQuery();
            while (r.next()) {
                
                arr.add(r.getInt("size"));
               
            }
            
            return arr;
  }
            catch (Exception e) {
            System.out.println(e);
        }
       return null;

  }*/
  /////////////////////////////////////////////////////////////////////ahmad
  
  
  
       public static ArrayList<Integer> selectcnturl(Connection c,String[]word) throws Exception {
        try { //get no of urls related
              String s="select count(distinct(url)) from search_eng where   ";
            
         
            ArrayList<Integer> arr = new ArrayList<Integer>();
        
            for (int i=0;i<word.length-1;i++)
            {
            s=s+"url in ( select url from search_eng where word ='"+word[i]+"') or ";
            }
            
            s=s+"url in (select url from search_eng where word = '"+word[word.length-1]+"');";
            PreparedStatement update = c.prepareStatement(s);
               ResultSet r = update.executeQuery();
            while (r.next()) {
                
                arr.add(r.getInt("count(distinct(url))"));
               
            }
            
            return arr;
            //System.out.println("selected");
 
        } catch (Exception e) {
            System.out.println(e);
        }
       return null;

    }
       /////////////////////////////////////////////////////////////////
       public static int[] pop (ArrayList<Integer> dblinks,int[]hlinks)
       { ///////////////////////////////////////////////////////////
       int[]cnt=new int[dblinks.size()];
       for (int i=0;i<cnt.length;i++)
       {
       cnt[i]=0;
       }
       for (int i=0;i<dblinks.size();i++)
           for (int j=0;j<hlinks.length;j++)
           {
           if (dblinks.get(i)==hlinks[j])
           {
               cnt[i]++;
           }
           }
       return cnt;
       }
       
       
         public static void updatesize(int x,Connection c) throws Exception { //not important
        try {
            
            PreparedStatement update = c.prepareStatement("update search_eng set size="+x+";");
            update.executeUpdate();
            //System.out.println("selected");
           
        } catch (Exception e) {
            System.out.println(e);
        }
       

    }
    
    private final BufferedReader read;
    private int indexer_count;
    private Connection c;

    public index(BufferedReader read, int indexer_count, Connection c) {
        this.read = read;
        this.indexer_count=indexer_count;
        this.c = c;
    }

    @Override
    public void run() {
        String[] stopwords = {"a", "about", "above", "above", "across", "after", "afterwards", "again", "against", "all", "almost",
            "alone", "along", "already", "also", "although", "always", "am", "among", "amongst", "amoungst", "amount", "an", "and",
            "another", "any", "anyhow", "anyone", "anything", "anyway", "anywhere", "are", "around", "as", "at", "back", "be", "became",
            "because", "become", "becomes", "becoming", "been", "before", "beforehand", "behind", "being", "below", "beside", "besides",
            "between", "beyond", "bill", "both", "bottom", "but", "by", "call", "can", "cannot", "cant", "co", "con", "could", "couldnt",
            "cry", "de", "describe", "detail", "do", "done", "down", "due", "during", "each", "eg", "eight", "either", "eleven", "else",
            "elsewhere", "empty", "enough", "etc", "even", "ever", "every", "everyone", "everything", "everywhere", "except", "few",
            "fifteen", "fify", "fill", "find", "fire", "first", "five", "for", "former", "formerly", "forty", "found", "four", "from",
            "front", "full", "further", "get", "give", "go", "had", "has", "hasnt",
            "have", "he", "hence", "her", "here", "hereafter", "hereby", "herein", "hereupon", "hers", "herself",
            "him", "himself", "his", "how", "however", "hundred", "ie", "if", "in", "inc", "indeed", "interest", "into",
            "is", "it", "its", "itself", "keep", "last", "latter", "latterly", "least", "less", "ltd", "made", "many",
            "may", "me", "meanwhile", "might", "mill", "mine", "more", "moreover", "most", "mostly", "move", "much", "must",
            "my", "myself", "name", "namely", "neither", "never", "nevertheless", "next", "nine", "no", "nobody", "none",
            "noone", "nor", "not", "nothing", "now", "nowhere", "of", "off", "often", "on", "once", "one", "only", "onto",
            "or", "other", "others", "otherwise", "our", "ours", "ourselves", "out", "over", "own", "part", "per", "perhaps",
            "please", "put", "rather", "re", "same", "see", "seem", "seemed", "seeming", "seems", "serious", "several", "she",
            "should", "show", "side", "since", "sincere", "six", "sixty", "so", "some", "somehow", "someone", "something",
            "sometime", "sometimes", "somewhere", "still", "such", "system", "take", "ten", "than", "that", "the", "their",
            "them", "themselves", "then", "thence", "there", "thereafter", "thereby", "therefore", "therein", "thereupon",
            "these", "they", "thickv", "thin", "third", "this", "those", "though", "three", "through", "throughout", "thru",
            "thus", "to", "together", "too", "top", "toward", "towards", "twelve", "twenty", "two", "un", "under", "until",
            "up", "upon", "us", "very", "via", "was", "we", "well", "were", "what", "whatever", "when", "whence", "whenever",
            "where", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which", "while",
            "whither", "who", "whoever", "whole", "whom", "whose", "why", "will", "with", "within", "without", "would", "yet",
            "you", "your", "yours", "yourself", "yourselves", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "1.", "2.", "3.", "4.", "5.", "6.", "11",
            "7.", "8.", "9.", "12", "13", "14", "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z",
            "terms", "CONDITIONS", "conditions", "values", "interested.", "care", "sure", ".", "!", "@", "#", "$", "%", "^", "&", "*", "(", ")", "{", "}", "[", "]", ":", ";", ",", "<", ".", ">", "/", "?", "_", "-", "+", "=",
            "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z",
            "contact", "grounds", "buyers", "tried", "said,", "plan", "value", "principle.", "forces", "sent:", "is,", "was", "like",
            "discussion", "tmus", "diffrent.", "layout", "area.", "thanks", "thankyou", "hello", "bye", "rise", "fell", "fall", "psqft.", "http://", "km", "miles"};

        Document doc;
       
        try  { //for each url
            Connection c=getconnection();
            ArrayList<String>s=geturlsdist(c);
            
            int current_line = 0;
            String url;
            while((url=read.readLine()) !=null )
            {
             current_line++;
             if(current_line > indexer_count){
            doc = Jsoup.connect(url).timeout(0).get();
            List<String> hyperlinks = new ArrayList<String>();
        Elements links = doc.select("a[href]");
    for(Element link : links)
    {
       System.out.println(link.attr("abs:href"));
       hyperlinks.add(link.attr("abs:href"));
     }
    
             
            int count = 0; 
         //      doc = Jsoup.connect(url).timeout(0).get();
            String title = doc.title();
            List<String> original_title_words = Arrays.asList(title.split(" "));
            count = original_title_words.size();
            List<String> final_words = new ArrayList<>();
            List<Integer> word_counts = new ArrayList<>();
            remove_stopwords(original_title_words, final_words, stopwords, word_counts);
            System.out.println(title);
            for (int i = 0; i < final_words.size(); i++) {
                // put in the database
              //  Thread.sleep(500);
             insert("title", final_words.get(i), url, word_counts.get(i),count,c);
               
           
                //ystem.out.println(word_count); 

            }
            final_words.clear();
            word_counts.clear();
            Elements hTags = doc.select("h1, h2, h3, h4, h5, h6");
            String headers = hTags.text();
            List<String> original_headers_words = Arrays.asList(headers.split(" "));
            count += original_headers_words.size();
            remove_stopwords(original_headers_words, final_words, stopwords, word_counts);
            // System.out.println("final words size: " +final_words.size());
            for (int i = 0; i < final_words.size(); i++) {
                // put in the database
               // Thread.sleep(500);
                insert("header", final_words.get(i), url, word_counts.get(i),count,c);
                 
                //ystem.out.println(word_count); 

            }
            //   System.out.println("final words number size: " +word_counts.size());
            final_words.clear();
            word_counts.clear();
            Elements pTags = doc.select("p");
            String paragraph = pTags.text();
            List<String> original_paragraph_words = Arrays.asList(paragraph.split(" "));
            count += original_paragraph_words.size();
            remove_stopwords(original_paragraph_words, final_words, stopwords, word_counts);
            System.out.println("final words size: " + final_words.size());
            System.out.println("final words number size: " + word_counts.size());
            for (int i = 0; i < final_words.size(); i++) {
                // put in the database(
                // Thread.sleep(500);
                insert("body", final_words.get(i), url, word_counts.get(i),count,c);
                  
                //ystem.out.println(word_count); 

            }
            indexer_count++;
            update(indexer_count,c); }
            }
        } catch (IOException ex) {
            Logger.getLogger(Indexer.class.getName()).log(Level.SEVERE, null, ex);
        } catch (Exception ex) {
            Logger.getLogger(index.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}

public class Indexer {

    /**
     * @param words
     * @param final_words
     * @param stopwords
     * @param word_count
     */
    public static void remove_stopwords(List<String> words, List<String> final_words, String[] stopwords, List<Integer> word_count) {
        englishStemmer stemmer = new englishStemmer();
        int is_stopword = 0;
        int temp;
        for (String word : words) {   //1-remove everything that is not a word
            //2-skep stopwords
            //
            word = word.replaceAll("[^\\w]", "");
            String wordCompare = word.toLowerCase(); //change the words to lowercase to compare with stemwords
            word = wordCompare;
            for (String stopword : stopwords) {
                if (wordCompare.equals(stopword)) {
                    is_stopword = 1;
                    break;
                }
            }
            if (is_stopword == 0) {
                stemmer.setCurrent(word);
                if (stemmer.stem()) {
                    String new_word = stemmer.getCurrent();
                    if (!final_words.contains(new_word)) //check if the word was included before
                    {

                        if (word.length() != 0) {
                            System.out.println(new_word);
                            final_words.add(new_word);
                            word_count.add(1);
                        }

                    } else { //if the word was included before we find where it was and then increment it`s count index
                        temp = word_count.get(final_words.indexOf(new_word));
                        temp++;
                        word_count.set(final_words.indexOf(new_word), temp);
                    }
                }

            }

            is_stopword = 0;    //reset is_stopword for the next word
        }

    }

    public static void main(String[] args) throws Exception {
         Connection c =getconnection();
         ArrayList<Integer>  index_count = selectx(c);
         System.out.println("index count: "+index_count.get(0));
         ArrayList<String>urls=geturlsdist(c);
           String[]s={"python"};
        List<String> testr = selector(s,c);
     System.out.println(testr); 
   
   //  String url="https://www.python.org/";
   for (int q=0;q<urls.size();q++)
   {
    List<Integer> test = getfreq(c,urls.get(q));
     int k=0; 
     for (int i=0;i<test.size();i++)
     {
    k=k+test.get(i);
     }
     insertlinks( urls.get(q), k, c);
     //ArrayList<Integer> nooc=selectcnturl( c,s);
     // float tf=0;
   //   tf=nooc.get(0)*100/k;
   //  List<Integer> test2=selectcnt(c);
     //System.out.println(tf);
    // double idf=log(test2.get(0)/test.get(0));
   }
     //System.out.println(idf+tf); 
       try {
            BufferedReader read = new BufferedReader(new FileReader("C:\\Users\\Ahmed Abd El Magid\\Documents\\NetBeansProjects\\Indexer\\crwaler\\school-master\\CrawlerSearchEngine\\school-master\\school-near\\CrawlerSearchEngine\\urls.txt"));
            long start_time = System.currentTimeMillis();
           Thread t1 = new Thread(new index(read,index_count.get(0),c));           
           t1.start();
           t1.join();
            c.close();
            long end_time = System.currentTimeMillis();
            long diff = end_time - start_time;
            System.out.println("time diff:" + diff);
        } catch (IOException e) {
            System.out.println("error opening file");
        }
        

    }


    
}