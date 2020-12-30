/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mongodbtest;


import com.mongodb.DB;  
import com.mongodb.DBCollection;  
import com.mongodb.DBCursor;  
import com.mongodb.DBObject;  
import com.mongodb.MongoClient;  
import java.util.List;  
import java.util.Set;  
import static java.util.concurrent.TimeUnit.SECONDS;  
import com.mongodb.util.JSON;

import java.net.UnknownHostException;
import java.util.Set;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

import com.mongodb.*;
import java.util.ArrayList;
//import twitter4j.internal.org.json.JSONObject;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.Proxy;
import java.net.URL;
import java.net.URLConnection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
/**
 *
 * @author Kallisto
 */
public class SaveToDataBase 
{
     /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws MalformedURLException, IOException {
        // TODO code application logic here
        
        


         try {

	Mongo mongo = new Mongo("localhost", 27017);
	DB db = mongo.getDB("myDB");

         System.out.printf("Got the database");
	SaveToDataBase simple = new SaveToDataBase();
	// get a single collection
	DBCollection collection = db.getCollection("tweetsCollection");
	System.out.println(collection.toString());

         System.out.printf("Got the collection");
        
	
        
        DBCollection collection2 = db.getCollection("userCollection");
        simple.clearDB( collection2);
         System.out.printf("Created userCollection");
        DBCollection collection3 = db.getCollection("tagsCollection");
        simple.clearDB( collection3);
         System.out.printf("Created tagsCollection");
        DBCollection collection4 = db.getCollection("urlCollection");
        simple.clearDB( collection4);
         System.out.printf("Created urlCollection");
        DBCollection collection5 = db.getCollection("RTUserCollection");
        simple.clearDB( collection5);
         System.out.printf("Created RTUserCollection");
        DBCollection collection6 = db.getCollection("RTTweetCollection");
        simple.clearDB( collection6);
         System.out.printf("Created RTTweetCollection");
      
         int numberOfUsers=0;
         ArrayList<String> theUsers = new ArrayList();
         
       
                        
	for( DBObject tweets : collection.find() )
        {
            numberOfUsers++;
            //Bγάζω τα 3 αρχικά (μαζί με το κενό) [" και τα 2 τελευταία "] (χωρις το κενο)
            String user =  tweets.get( "user" ).toString();
            //user = user.substring(3);
          //  user = user.substring(0, user.length() - 2);
            user = user.replace("\"", "");
            user = user.replace("[","");
            user = user.replace("]","");
            theUsers.add(user);
            
            String tweet =  tweets.get( "tweet" ).toString();
           // tweet = tweet.substring(3);
           // tweet = tweet.substring(0, tweet.length() - 2);
            tweet = tweet.replace("\"", "");
            tweet = tweet.replace("[","");
            tweet = tweet.replace("]","");
            
            String date =  tweets.get( "date" ).toString();
           // date = date.substring(3);
           // date = date.substring(0, date.length() - 2);
            date = date.replace("\"", "");
            date = date.replace("[","");
           date = date.replace("]","");
            
            System.out.println("user = "+user);
            System.out.println("tweet = "+tweet);
            System.out.println("date = "+date);
            
            String[] tweetAr = tweet.split(" "); //Χώρισε το tweet σε λέξεις με βάση το κενό
            ArrayList<String> listOfWords = new ArrayList();
            
            for(int ss=0;ss<tweetAr.length;ss++)
            {
                listOfWords.add(tweetAr[ss]);
            }
            
            int rt=0; //δεν είναι retweet
            int ur=0; // δεν εχει url
            int tg=0; //δεν εχει tags
            int mu=0; //δεν εχει metioned users
            ArrayList<String> rtUsers = new ArrayList(); //λίστα για τους χρήστες που κάνει retweet

            String User;
            String https=null;
            ArrayList<String> Tags = new ArrayList(); //λίστα για τα tags Που περιέχει το tweet
            String longHttps=null;
            ArrayList<String> Url = new ArrayList(); //λίστα για τα tags Που περιέχει τα urls 
            ArrayList<String> LongUrl = new ArrayList();
            
            
            for( int i=0;i<listOfWords.size();i++)
            {
               
                
                if(listOfWords.get(i).contains("\\n")) //αν έχει \\n ή \n σημαίνει πως αλλάζει η γραμμή και πολλές φορές αυτό ενώνει λέξεις
                {
                   // System.out.println("Here!!!!!!!!!!!!!!!");
                 
                    String replaceW = listOfWords.get(i);
                    replaceW= replaceW.replace("\\n"," "); //το αντικαθιστώ με κενό για να χωρίσω τις λέξεις
                    //System.out.println(replaceW);
                    String[] tweetAr2 = replaceW.split(" ");//τις ξαναχωρίζω 
                    int size=tweetAr2.length;
                    for(int ff=0;ff<size;ff++)
                    {
                        String a = tweetAr2[ff];
                        if(a==" ")
                        {
                            
                        }else
                        {
                        listOfWords.add(a);//και τις βάζω μέσα στη λίστα με τις λέξεις για αργότερο έλεγχο
                        
                         }
                    }
                }
                else
                {
                        if(listOfWords.get(i).contains("RT")) //αν υπάρχει λέξη "RT" σημαίνει πως είναι retweet
                        {
                              rt=1; // είναι retweet                    
                        }
                        
                        
                      
                    if(listOfWords.get(i).contains("@"))
                            { 
                                mu=1;
                               String rtUser=listOfWords.get(i);
                               rtUser = rtUser.replace(":"," ");
                            //   rtUser = rtUser.substring(0, rtUser.length() - 1); //βγάω την : στο τελος καθε ονόματος χρήστη
                               rtUsers.add(rtUser);
                            }
                       
                    if(listOfWords.get(i).contains("#"))
                            {
                                tg=1;
                                //System.out.println("HERE!!!!!!!!!!!!!!!!!!!!!!");
                                String tag = listOfWords.get(i);
                                
                                
                                //αν υπάρχουν πολλά hastags κολλημένα μαζί πρέπει να τα χωρίσω
                                int size=tag.length();
                                int count=0;
                                for(int d=0;d<size;d++)
                                {
                                    if(tag.charAt(d)=='#')//βλεπει καθε χαρακτηρα αν είναι #
                                    {
                                        count++;//και μετρά πόσα # θα βρει
                                    }
                                }
                                
                                if (count > 1) //αν βρει πάνω από 1 τότε σημαίνει πως είναι κολλημένα μεταξύ τους πάνω από 1
                                {
                                    //System.out.println("HERE222!!!!!!!!!!!!!!!!!!!!!!");
                                            
                                    String[] tags = tag.split("#");
                                    for(int t=1;t<tags.length;t++)
                                    {
                                        System.out.println("tags[t]="+tags[t]);
                                        //η πρώτη θέση είναι κενή
                                        Tags.add(tags[t]);
                                    }
                                    
                                }
                                else
                                {
                                    Tags.add(tag);
                                }
                            }                             
               
                          if(listOfWords.get(i).contains("http"))
                            {
                                ur=1;
                                https=listOfWords.get(i);
                                
                                ArrayList links = new ArrayList();
 
                                //String regex = "\\(?\\b(http://|www[.])(https://|www[.])[-A-Za-z0-9+&amp;@#/%?=~_()|!:,.;]*[-A-Za-z0-9+&amp;@#/%=~_()|]";
                               Pattern p = Pattern.compile(
        "(?:^|[\\W])((ht|f)tp(s?):\\/\\/|www\\.)"
                + "(([\\w\\-]+\\.){1,}?([\\w\\-.~]+\\/?)*"
                + "[\\p{Alnum}.,%_=?&#\\-+()\\[\\]\\*$~@!:/{};']*)",
        Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL);
                                //Pattern p = Pattern.compile(regex);
                                Matcher m = p.matcher(https);
                               
                                while(m.find())
                                   {
                                       
                                       String urlStr = m.group();
                                       if (urlStr.startsWith("(") && urlStr.endsWith(")"))
                                          {
                                            urlStr = urlStr.substring(1, urlStr.length() - 1);
                                          }
                                       else if (urlStr.startsWith("("))
                                          {
                                             urlStr = urlStr.substring(0, urlStr.length());
                                           }
                                        else if ( urlStr.endsWith(")"))
                                           {
                                              urlStr = urlStr.substring(0, urlStr.length() - 1);
                                           }
                                        
                                         links.add(urlStr);
                                    }
             
                                for (int g=0;g<links.size();g++)
                                    {
                                      https=(String) links.get(g);
                                     // System.out.println("link = "+https);
                                     
                                     
                                    //ελέγχει αν το url είναι short ή όχι. αν ναι τότε θα το στείλει για πάρει το κανονικό
                                    if(https.contains("bit.ly"))
                                       {
                                        longHttps = simple.getLongUrl(https);
                                       }
                                    else if(https.contains("t.co"))
                                       {
                                        longHttps = simple.getLongUrl(https);
                                       }
                                    else
                                       {
                                         longHttps = https;
                                         https=null;
                                       }
                    
                                       Url.add(https);
                                       LongUrl.add(longHttps);
                    
                                     }
                           }
            } 
            
        }
            
            
          
        System.out.println("Is it retweet? Yes=1 No=0 : "+rt);
        
        for(int k=0;k<rtUsers.size();k++)
        {
            System.out.println("Metioned user = " + rtUsers.get(k));
        }
       
        for(int j=0;j<Tags.size();j++)
        {
            System.out.println("tag = " + Tags.get(j));
        }
        
      
        
        
        for(int j1=0;j1< LongUrl.size();j1++)
        {
            System.out.println("link ext = " + LongUrl.get(j1));
        }
        
        for(int j2=0;j2<Url.size();j2++)
        {
            System.out.println("link = " + Url.get(j2));
        }
        
        
        
        //Θα έχω 5 collections ( τις έχω φτιάξει στην αρχή) 
        
        
        //1η User με όλα τα στοιχεία του σχετικά με tags,urls,Retweets,Retweeted users
        
        
        BasicDBObject obj2 = new BasicDBObject();
          
        
        obj2.append("user", user);
        obj2.append("tweet", tweet);
        obj2.append("timestamp", date);
        obj2.append("shortHttps",https);
        obj2.append("longHttps", longHttps);
        
      
        BasicDBList listRTUsers = new BasicDBList();
        
          
                for (int h=0;h<rtUsers.size();h++)
                {
                   listRTUsers.add( new BasicDBObject()
                        .append("MetionedUser",rtUsers.get(h)));
                }
                
        BasicDBList listTags = new BasicDBList();
        
          
                for (int e=0;e<Tags.size();e++)
                {
                   listTags.add( new BasicDBObject()
                        .append("tag", Tags.get(e)));
                }
                
                
          BasicDBList ListUrl = new BasicDBList();
        
                for (int h=0;h<Url.size();h++)
                {
                   ListUrl.add( new BasicDBObject()
                        .append("shortUrl",Url.get(h)));
                }
                
                
                
            BasicDBList LongUrls = new BasicDBList();
        
          
                for (int h=0;h<LongUrl.size();h++)
                {
                   LongUrls.add( new BasicDBObject()
                        .append("longUrl",LongUrl.get(h)));
                }
                    
        obj2.append("ListUrls", listRTUsers);
        obj2.append("LongUrls", listRTUsers);
        obj2.append("ListRTUsers", listRTUsers);    
        obj2.append("ListOfTags", listTags); 
        Object o2 = com.mongodb.util.JSON.parse(obj2.toString());
        DBObject dbObj2 = (DBObject) o2;
        collection2.insert(dbObj2);  
        
      //  simple.show( collection2);
        
       
        
        
        //2η hastags
        
        System.out.printf("HASTAG-COLLECTION");
        
        if(tg==1)//αν περιέχει tags
        {
        
        for (int tag=0;tag<Tags.size();tag++)
        {
             BasicDBObject obj3 = new BasicDBObject();
              obj3.append("user", user);
              obj3.append("timestamp", date);
              obj3.append("tag",Tags.get(tag));
              
               Object o3 = com.mongodb.util.JSON.parse(obj3.toString());
               DBObject dbObj3 = (DBObject) o3;
               collection3.insert(dbObj3);  
        }
       
        
        }
       
        
       // simple.show(collection3);
        
        
        
        
        //3η URLs
        
         System.out.printf("URLS-COLLECTION");
         

         if(ur==1)//Aν περιέχει urls
         {
        for(int url=0;url<Url.size();url++)
        {
            BasicDBObject obj4 = new BasicDBObject();
        
            obj4.append("user", user);
            obj4.append("timestamp", date);
            obj4.append("shortUrl", Url.get(url));
            obj4.append("longUrl",LongUrl.get(url));
        
        
            Object o4 = com.mongodb.util.JSON.parse(obj4.toString());
            DBObject dbObj4 = (DBObject) o4;
            collection4.insert(dbObj4);  
        }
         }
        
                
           
        
        
        
        
        //4η Retweed User or metioned users
        System.out.printf("MetionedUsers-COLLECTION");
        
        if(mu==1) //αν έχει metioned users
        {
            
        
        for(int mtu=0;mtu<rtUsers.size();mtu++)
        {
            BasicDBObject obj5 = new BasicDBObject();
          
        
            obj5.append("user", user);
            obj5.append("timestamp", date);
            obj5.append("MetionedUser",rtUsers.get(mtu));
             Object o5 = com.mongodb.util.JSON.parse(obj5.toString());
        DBObject dbObj5 = (DBObject) o5;
        collection5.insert(dbObj5); 
            
        }
        
        }
        
        
        
         
       // simple.show( collection5);
        
        
        
        
        
        //5η Retweet
        System.out.printf("RETWEET-COLLECTION");
        BasicDBObject obj6 = new BasicDBObject();
          
        if(rt==1) //if there is a retweet
        {
        obj6.append("user", user);
        obj6.append("timestamp", date);
        obj6.append("retweet", tweet);
        
        Object o6 = com.mongodb.util.JSON.parse(obj6.toString());
        DBObject dbObj6 = (DBObject) o6;
        collection6.insert(dbObj6); 
        }
        
       // simple.show( collection6);
        
        
        
 
  
	}
     
        System.out.println("Finished with all the entries!");
        
        System.out.println("All collections!");
        simple.show(collection2);
        System.out.println("Tags Collection!");
        simple.show(collection3);
        System.out.println("Urls Collection!");
        simple.show(collection4);
        System.out.println("Retweeted Users");
        simple.show(collection5);
        System.out.println("Retweet Collection!");
        simple.show(collection6);
         //Check if there is any user twice or more in the list and take the extra entried out
         //And add them to a collection
        
        

        
        
        
        for(int f=0;f<theUsers.size();f++) //για καθε user
        {
           
            int d=f+1;// θα ξεκινησεις να ελεγχει απο την επομενη θεση, δλδ τη 2
        
            while(d<theUsers.size()) // όσο το d < ή = με το size της λιστας
            {
                
             if(theUsers.get(f)==theUsers.get(d)) //ε΄λεγξε αν ο χρηστης υπαρχει ξανα μεσα στη λιστα
                {
            
                    theUsers.remove(d);      // αν ναι βγαλε την extra θεση
                }
             d++;//και πανε στην επομενη
        
            }
            d=0;
        }
        
              
        System.out.println("The NEW list with the users -> COUNT="+ theUsers.size());
     
        
     
        
        
    
    
                
        
        
        
        
        
        
        

    } catch (MongoException e) {
	e.printStackTrace();
    }

    
         
         
 }


    
    
    
    
    // κάνει μεγαλύτερο το url
    public static String getLongUrl(String shortUrl) throws MalformedURLException, IOException {
       String result = shortUrl;
        String header;
            do {
                URL url = new URL(result);
                HttpURLConnection.setFollowRedirects(false);
                URLConnection conn = url.openConnection();
                header = conn.getHeaderField(null);
                String location = conn.getHeaderField("location");
                if (location != null) {
                    result = location;
                }
            } while (header.contains("301"));
 
        return result;
        
        /*URL url = new URL(shortUrl);

        HttpURLConnection connection = (HttpURLConnection) url.openConnection(Proxy.NO_PROXY); //using proxy may increase latency
        connection.setInstanceFollowRedirects(false);
        connection.connect();
        String expandedURL = connection.getHeaderField("Location");
        connection.getInputStream().close();
        System.out.println("expandedURL="+expandedURL);
                
        return expandedURL;*/
     
    

     }


    
    
     public void show(DBCollection tweets)
    {
        
          DBCursor cursorDocJSON = tweets.find();
                        
	while (cursorDocJSON.hasNext()) 
        {
           System.out.println(cursorDocJSON.next());
	}
        
        
    }

     
     public void clearDB(DBCollection tweets)
    {
        DBCursor cursorDocJSON = tweets.find();
                        
	while (cursorDocJSON.hasNext()) 
        {
           tweets.remove(cursorDocJSON.next());
	}
       System.out.println("Ok! Empty!");
    }
    
   
}
    
    
    
    
    
    

