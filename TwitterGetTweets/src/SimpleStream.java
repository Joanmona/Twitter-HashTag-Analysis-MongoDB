/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author Kallisto
 */


import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.User;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.TwitterException;
import java.io.Serializable;

import com.mongodb.DBCursor; 
import com.mongodb.DB;  
import com.mongodb.DBCollection;    
import com.mongodb.DBObject;  
import com.mongodb.MongoClient;  
import java.util.List;  
import java.util.Set;  
import static java.util.concurrent.TimeUnit.SECONDS;  
import com.mongodb.util.JSON;
import twitter4j.json.DataObjectFactory;
import com.mongodb.*;
import java.io.IOException;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import twitter4j.HashtagEntity;
import twitter4j.JSONException;
import twitter4j.JSONObject;



public class SimpleStream 
{
    
    public static int id=0;
    
    public SimpleStream()
    {}
    public static void main(String[] args) throws TwitterException, IOException 
    {
        
        // To directly connect to a single MongoDB server (note that this will not auto-discover the primary even
      
      try{   
		
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true);
        cb.setOAuthConsumerKey("PknAQAPZNT4MbpIk16NtKHX0F");
        cb.setOAuthConsumerSecret("fyIQSWyM6LkDTJTZZVztNQ1bvpW54lMMIFwyAfEtKnjmyOmENu");
        cb.setOAuthAccessToken("805393178648018944-9H25u28cRjGGdSfcg0ulSc1TKE3BEpy");
        cb.setOAuthAccessTokenSecret("5pEXqYdVG2DaA5T7vZUBqoS9uaT4LLlkKbvkBISsy4zwb");
         // To connect to mongodb server
         MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
			
         // Now connect to your databases
         DB db = mongoClient.getDB( "myDB" );
         System.out.println("Connect to database successfully");
			        
         DBCollection tweets = db.getCollection("tweetsCollection");
         System.out.println("Collection tweets selected successfully");
		
        
        
         
         TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
         SimpleStream simple = new SimpleStream();
                simple.clearDB( tweets); 
             
         StatusListener listener = new StatusListener()
            {
             
            @Override
            public void onException(Exception arg0) {/*TODO Auto-generated method stub*/}

            @Override
            public void onDeletionNotice(StatusDeletionNotice arg0) {/* TODO Auto-generated method stub*/}

            @Override
            public void onScrubGeo(long arg0, long arg1) {/*TODO Auto-generated method stub*/}

            @Override
            public void onStatus(Status status) {
                
                
                JSONObject jsonObj = new JSONObject();

                id=id+1;
                
                User user = status.getUser();
                // gets Username
                String username = status.getUser().getScreenName();
             //   System.out.println(username);
                String profileLocation = user.getLocation();
              //  System.out.println(profileLocation);
                long tweetId = status.getId(); 
               // System.out.println(tweetId);
                String content = status.getText();
             //  System.out.println(content +"\n");
                Date time = status.getCreatedAt();
                
               

                    
                 try {
                    jsonObj.append("id",id);
                    jsonObj.append("user", username);
                    jsonObj.append("location", profileLocation);
                    jsonObj.append("tweetid", tweetId);
                    jsonObj.append("tweet", content);
                    jsonObj.append("date", time);
                    
                   
                } catch (JSONException ex) {
                    Logger.getLogger(SimpleStream.class.getName()).log(Level.SEVERE, null, ex);
                }
                
                Object o = com.mongodb.util.JSON.parse(jsonObj.toString());
                DBObject dbObj = (DBObject) o;
            
                
                tweets.insert(dbObj);
                  
                  if(id>100)
                    {
                     simple.show( tweets); 
                    
                     
                     
                     
                     
                     
                     
                     
                      twitterStream.cleanUp();
                    }
                   
            }

            @Override
            public void onTrackLimitationNotice(int arg0) {
                // TODO Auto-generated method stub

            }

            @Override
            public void onStallWarning(StallWarning sw) {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

        };
             
             
        
        
        FilterQuery fq = new FilterQuery();
        String keywords[] = {"#harrypotter","#NewYear","#RIP","#marvel","#moana"};
        fq.track(keywords);
       
        
        
        twitterStream.addListener(listener);
        twitterStream.filter(fq);
       
        
				
        // coll.insert(doc);
      //   System.out.println("Document inserted successfully");
      }catch(Exception e){
         System.err.println( e.getClass().getName() + ": " + e.getMessage() );
      }
   }
    
    
    public void show(DBCollection tweets)
    {
        
          DBCursor cursorDocJSON = tweets.find();
                        System.out.println("HEY2!");
	while (cursorDocJSON.hasNext()) 
        {
           System.out.println(cursorDocJSON.next());
	}
        
        System.out.println("That's all!");
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
       
             