/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package analisisarrays;

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
import java.io.File;
import java.util.ArrayList;
//import twitter4j.internal.org.json.JSONObject;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;
/**
 *
 * @author Kallisto
 */
public class AnalisisArrays {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) 
    {
        // TODO code application logic here
         try {

	Mongo mongo = new Mongo("localhost", 27017);
	DB db = mongo.getDB("myDB");

         System.out.printf("Got the database");
	AnalisisArrays simple = new AnalisisArrays();
	// get a single collection
	DBCollection userCollection = db.getCollection("userCollection");
	System.out.println(userCollection.toString());
        
        DBCollection tagsCollection = db.getCollection("tagsCollection");
	System.out.println(tagsCollection.toString());
        
        DBCollection urlCollection = db.getCollection("urlCollection");
	System.out.println(urlCollection.toString());
        
        DBCollection RTUserCollection = db.getCollection("RTUserCollection");
	System.out.println(RTUserCollection.toString());
        
        DBCollection RTTweetCollection = db.getCollection("RTTweetCollection");
	System.out.println(RTTweetCollection.toString());
       
        
        
        
      ////////////////////////////TAGS////////////////////////////////////////////////  
        
        
        
        
        
       //Θα φτιάξω ένα hashmap για κάθε χρήστη και σε κάθε χρήστη θα αντιστοιχεί μία λίστα με τα tags του
        HashMap<String,ArrayList<String>> UserTags =new HashMap<>();
        
        ArrayList <String> UserList = new ArrayList();
        for( DBObject tags : tagsCollection.find() )
        {
             
             //Για κάθε νέο αντικείμενο πάρε το όνομα του user
             String User = tags.get("user").toString();
             
             if(UserTags.containsKey(User))
             {
                 //System.out.println("This user already exists");
             }
             else
             {
         //       System.out.println("User="+User);
             
             //φτιαξε ένα query με τα στοιχεία που θέλεις, δλδ το όνομα του user να είναι αυτό  που πήρες από πάνω
               BasicDBObject query = new BasicDBObject();
               query.put("user", User);
               DBCursor cursor = tagsCollection.find(query);

               ArrayList<String> Tags = new ArrayList();
               while(cursor.hasNext()) //βρες όλα τα entries με όνομα "user"=User
                    {
                        String tag = cursor.next().get("tag").toString();
                        tag = tag.replace("#", "");//βγάλε το # 
                
                        Tags.add(tag);//πάρε τα tags και βάλε τα σε μια λίστα
                
                    }
                UserTags.put(User, Tags);// κάθε χρήστης έχει από μία λίστα με τα tags του
                UserList.add(User);
             }
            
                  
        }
  
        
            
        //Τώρα θα φτιάξω τον πίνακα ομοιότητας των χρηστών ανάλογα τα tags τους
        //Θα φτιάξω ένα hashmap όπου σε κάθε θέση  θα είναι ο χρήστης στον οποίο θα αντιστοιχεί ένα άλλο hashmap
        // που θα περιλαμβάνει όλους τους χρήστες πάλι με τον βαθμό ομοιότητάς τους με τον αρχικό χρήστη
        
        
        //Jaccard
        
        HashMap<String,HashMap<String,Double>> TagsTable = new HashMap();
            
                
    for (int hash=0;hash<UserList.size();hash++)//για κάθε user
       {
                String User1 = UserList.get(hash);
          
            
            //Παίρνω τη λίστα με τα tags του κάθε χρήστη
                ArrayList<String> tags1 = new ArrayList();
                tags1=UserTags.get(User1);
            
            
                 HashMap<String,Double>  TagsSimilarity = new HashMap();
                 
            //Τώρα παίρνω τα tags του κάθε άλλου χρήστη που θα συγκρίνω με τον πρώτο
             for (int hash2=0;hash2<UserList.size();hash2++)//για κάθε user
              {
                 String User2 = UserList.get(hash2);
               
                  ArrayList<String> tags2 = new ArrayList();
                  tags2=UserTags.get(User2);
                 
                  
                   int inter=0;

                  //για καθε tag του χρηστη 1 ελεγχει αν ειναι ιδιο με καθε tag Του χρηστη 2
                   for(int w=0;w<tags1.size();w++)
                   {
                       for(int ww=0;ww<tags2.size();ww++)
                       {
                           if(tags2.get(ww).equalsIgnoreCase(tags1.get(w)))
                           {
                              inter++; //τα κοινα tags 
                           }
                       }
                   }
              
                   int alltags = tags1.size() + tags2.size() - inter;// όλα τα tags και των 2 χρηστών μαζι - τα κοινα μια φορα
         
                   double similarity = (double) inter/alltags;
         
                   TagsSimilarity.put(User2,similarity);
                 
                  
             }// end user2 
             
             
             TagsTable.put(User1,TagsSimilarity);
        }//end user1
        
               
        
        //////////////////////////////METIONED USERS////////////////////////////////
        
         //Θα φτιάξω ένα hashmap για κάθε χρήστη και σε κάθε χρήστη θα αντιστοιχεί μία λίστα με τους mentioned users
        HashMap<String,ArrayList<String>> UserMetionedUsers =new HashMap<>();
        
        ArrayList <String> UserList2 = new ArrayList(); //λιστα με ολους τους χρήστες
        
        for( DBObject rtusers : RTUserCollection.find() )
        {
             
             //Για κάθε νέο αντικείμενο πάρε το όνομα του user
             String User = rtusers.get("user").toString();
            
             if(UserMetionedUsers.containsKey(User))
             {
                // System.out.println("This user already exists");
             }
             else
             {
                
             
             //φτιαξε ένα query με τα στοιχεία που θέλεις, δλδ το όνομα του user να είναι αυτό  που πήρες από πάνω
             BasicDBObject query = new BasicDBObject();
             query.put("user", User);
             DBCursor cursor = RTUserCollection.find(query);

             ArrayList<String> rtUsers = new ArrayList(); //λίστα με τους metioned users για καθε χρηστη
             while(cursor.hasNext()) //βρες όλα τα entries με όνομα "user"=User
             {
                String rtU = cursor.next().get("MetionedUser").toString();
              
             
                
                if(rtU.equals("@"))//πολλοί mentioned users είναι κενοί γτ οι κατσαπιάδες αφήνανε κενό μεταξύ του @ και του user
                {
               // System.out.println("it just a @");
                }else
                {
                     rtUsers.add(rtU);//πάρε τoυς metioned users και βάλε τα σε μια λίστα
                }
               
                
             }
             
              UserMetionedUsers.put(User, rtUsers);// κάθε χρήστης έχει από μία λίστα με τα tags του
              UserList2.add(User);// αφου τελειωσα με αυτον τον χρηστη τον βαζω στη λιστα
             }
            
                  
        }
        
                     
        
         //Τώρα θα φτιάξω τον πίνακα ομοιότητας των χρηστών ανάλογα τους χρήστες που αναφέρουν στα tweets τους
        //Θα φτιάξω ένα hashmap όπου σε κάθε θέση  θα είναι ο χρήστης στον οποίο θα αντιστοιχεί ένα άλλο hashmap
        // που θα περιλαμβάνει όλους τους χρήστες πάλι με τον βαθμό ομοιότητάς τους με τον αρχικό χρήστη
        
        
        //Jaccard
        
        HashMap<String,HashMap<String,Double>> MetionedUsersTable = new HashMap();
   
        
        for (int hash=0;hash<UserList2.size();hash++)//για κάθε user
        {
            String User1 = UserList2.get(hash);
            
            
            //Παίρνω τη λίστα με τα tags του κάθε χρήστη
            ArrayList<String> metUsers1 = new ArrayList();
            metUsers1=UserMetionedUsers.get(User1);
          
            
            HashMap<String,Double>  MetionedUsersSimilarity = new HashMap();
            
            //Τώρα παίρνω τα tags του κάθε άλλου χρήστη που θα συγκρίνω με τον πρώτο
             for (int hash2=0;hash2<UserList2.size();hash2++)//για κάθε user
              {
                  String User2 = UserList2.get(hash2);
                  ArrayList<String> metUsers2 = new ArrayList();
                  metUsers2=UserMetionedUsers.get(User2);
                  
                  
                   int inter=0;
                   
                 
                  //για καθε tag του χρηστη 1 ελεγχει αν ειναι ιδιο με καθε tag Του χρηστη 2
                   for(int w=0;w<metUsers1.size();w++)
                   {
                       for(int ww=0;ww<metUsers2.size();ww++)
                       {
                          if(metUsers2.get(ww).equals(metUsers1.get(w)))
                          {
                             inter++; //οι κοινοί mentioned users
                          }
                      }
                   }
             
                   
                   int allmetionedusers = metUsers1.size() + metUsers2.size() - inter;// όλα τα tags και των 2 χρηστών μαζι - τα κοινα μια φορα
               
                   double similarity = (double) inter/allmetionedusers;
                                
                  
                   MetionedUsersSimilarity.put(User2,similarity); 
             }
            
              
              MetionedUsersTable.put(User1,MetionedUsersSimilarity);
        }
              
        
        
              
        
           
        
        //////////////////////////////URLS/////////////////////////////////////
        
        
     //Θα φτιάξω ένα hashmap για κάθε χρήστη και σε κάθε χρήστη θα αντιστοιχεί μία λίστα με τα long urls του
        HashMap<String,ArrayList<String>> UrlsUsers = new HashMap<>();
        ArrayList <String> UserList3 = new ArrayList(); //λιστα με ολους τους χρήστες
        for( DBObject urls : urlCollection.find() )
        {
             
             //Για κάθε νέο αντικείμενο πάρε το όνομα του user
             String User3 = urls.get("user").toString();
         
             
             if(UrlsUsers.containsKey(User3))
             {
                //  System.out.println("This user already exists");
             }
             else
             {
       
             
             //φτιαξε ένα query με τα στοιχεία που θέλεις, δλδ το όνομα του user να είναι αυτό  που πήρες από πάνω
             BasicDBObject query1 = new BasicDBObject();
             query1.put("user", User3);
             DBCursor cursor = urlCollection.find(query1);

             ArrayList<String> urlsList = new ArrayList(); //λίστα με τα urls για καθε χρηστη
             while(cursor.hasNext()) //βρες όλα τα entries με όνομα "user"=User
             {
                String U = cursor.next().get("shortUrl").toString();//παίρνω τα short γτ μπορεί κάποια να μην μπόρεσαν να γίνουν expand
                urlsList.add(U);//πάρε τoυς metioned users και βάλε τα σε μια λίστα
                
             }
             
             UrlsUsers.put(User3, urlsList);// κάθε χρήστης έχει από μία λίστα με τα tags του
              UserList3.add(User3);// αφου τελειωσα με αυτον τον χρηστη τον βαζω στη λιστα
             }
            
                  
        }
     
  
                 
        
        
         //Τώρα θα φτιάξω τον πίνακα ομοιότητας των χρηστών ανάλογα τα urls στα tweets τους
        //Θα φτιάξω ένα hashmap όπου σε κάθε θέση  θα είναι ο χρήστης στον οποίο θα αντιστοιχεί ένα άλλο hashmap
        // που θα περιλαμβάνει όλους τους χρήστες πάλι με τον βαθμό ομοιότητάς τους με τον αρχικό χρήστη
        
        
        //Jaccard
        
        HashMap<String,HashMap<String,Double>> UrlsTable = new HashMap();

       
        for (int hash=0;hash<UserList3.size();hash++)//για κάθε user
        {
        
            String User1 = UserList3.get(hash);
            
            //Παίρνω τη λίστα με τα url του κάθε χρήστη
            ArrayList<String> urlUsers1 = new ArrayList();
            urlUsers1=UrlsUsers.get(User1);            
            
             HashMap<String,Double>  UrlsSimilarity = new HashMap();
             
            //Τώρα παίρνω τα url του κάθε άλλου χρήστη που θα συγκρίνω με τον πρώτο
             for (int hash2=0;hash2<UserList3.size();hash2++)//για κάθε user
              {
                 String User2 = UserList3.get(hash2);
             
                  ArrayList<String> urlUsers2 = new ArrayList();
                  urlUsers2=UrlsUsers.get(User2);//παίρνει τη λιστα με τα Url του χρήστη usre 2
            
                   int inter=0;
                   
                  //για καθε tag του χρηστη 1 ελεγχει αν ειναι ιδιο με καθε url Του χρηστη 2
                   for(int w=0;w<urlUsers1.size();w++)
                   {
                   
                       for(int ww=0;ww<urlUsers2.size();ww++)
                       {
                           if(urlUsers2.get(ww).equals(urlUsers1.get(w)))
                           {
                              inter++; //τα κοινα url 
                           }
                       }
                   }
                
                   
                   int allurls = urlUsers1.size() + urlUsers2.size() - inter;// όλα τα tags και των 2 χρηστών μαζι - τα κοινα μια φορα
               
                   double similarity = (double) inter/allurls;
              
                   UrlsSimilarity.put(User2,similarity );
                 
             }
            
            UrlsTable.put(User1,UrlsSimilarity);
        }
        
        
        
          //////////////////////////////RE-TWEETS/////////////////////////////////////
        
        
     //Θα φτιάξω ένα hashmap για κάθε χρήστη και σε κάθε χρήστη θα αντιστοιχεί μία λίστα με τα retweets του
        HashMap<String,ArrayList<String>> ReTweetUsers = new HashMap<>();
        
        ArrayList <String> UserList4 = new ArrayList(); //λιστα με ολους τους χρήστες
        for( DBObject urls : RTTweetCollection.find() )
        {
             
             //Για κάθε νέο αντικείμενο πάρε το όνομα του user
             String User4 = urls.get("user").toString();
             
             if(ReTweetUsers.containsKey(User4))
             {
            //     System.out.println("This user already exists");
             }
             else
             {
           
             
             //φτιαξε ένα query με τα στοιχεία που θέλεις, δλδ το όνομα του user να είναι αυτό  που πήρες από πάνω
             BasicDBObject query2 = new BasicDBObject();
             query2.put("user", User4);
             DBCursor cursor = RTTweetCollection.find(query2);

             ArrayList<String> retweetsList = new ArrayList();
             //λίστα με τα retweets για καθε χρηστη
             while(cursor.hasNext()) //βρες όλα τα entries με όνομα "user"=User
             {
                 String rtw = cursor.next().get("retweet").toString();
                 rtw = rtw.replace("RT", " ");//βγάλε το RT
         
                 retweetsList.add(rtw);//πάρε τoυς metioned users και βάλε τα σε μια λίστα
                
             }
              ReTweetUsers.put(User4, retweetsList);// κάθε χρήστης έχει από μία λίστα με τα tags του
              UserList4.add(User4);// αφου τελειωσα με αυτον τον χρηστη τον βαζω στη λιστα
             }
            
                  
        }
   
                 
        
        
         //Τώρα θα φτιάξω τον πίνακα ομοιότητας των χρηστών ανάλογα τα retweet τους
        //Θα φτιάξω ένα hashmap όπου σε κάθε θέση  θα είναι ο χρήστης στον οποίο θα αντιστοιχεί ένα άλλο hashmap
        // που θα περιλαμβάνει όλους τους χρήστες πάλι με τον βαθμό ομοιότητάς τους με τον αρχικό χρήστη
        
        
        //Jaccard
        
        HashMap<String,HashMap<String,Double>> RetweetsTable = new HashMap();

       
        
        //Τώρα παίρνω τα rwtweets του κάθε άλλου χρήστη που θα συγκρίνω με τον επομενο
        for (int hash=0;hash<UserList4.size();hash++)//για κάθε user 
        {
         
             String User1 = UserList4.get(hash);
            
            //Παίρνω τη λίστα με τα retweets του κάθε χρήστη
            ArrayList<String> rtwUsers1 = new ArrayList();
            rtwUsers1=ReTweetUsers.get(User1);
            
      
            
             HashMap<String,Double>  RetweetSimilarity = new HashMap();
             
            //Τώρα παίρνω τα rwtweets του κάθε άλλου χρήστη που θα συγκρίνω με τον πρώτο
             for (int hash2=0;hash2<UserList4.size();hash2++)//για κάθε user
              {
             
                  
                
                  String User2 = UserList4.get(hash2);
              
                  ArrayList<String> rtwUsers2 = new ArrayList();
                  rtwUsers2=ReTweetUsers.get(User2);
 
                   
                  /////Μέχρι εδώ έχω τις λίστες και με τα retweets και από τους 2 χρήστες
                  
                  
                  
                  ArrayList<Double> simi = new ArrayList(); // οι ομοιότητες με όλα τα rt του 1 με όλα τα rt του 2
                  int allRt=0;
                   for (int q=0;q<rtwUsers1.size();q++)// για retweet της λίστας του πρώτου χρηστη
                           { 
                                // χώρισέ το σε tokens
                                String str1 = rtwUsers1.get(q);
                                System.out.println("RT1= "+str1);
                                StringTokenizer defaultTokenizer = new StringTokenizer(str1);
                                ArrayList<String> tokens1 = new ArrayList();
                                while (defaultTokenizer.hasMoreTokens())
                                        {
                                         tokens1.add(defaultTokenizer.nextToken());
                                        }
                       
                                ArrayList<Double> similar = new ArrayList();// λίστα ομοιόητας του καθε RT του χρήστη1 με κάθε RT το χρηστη 2
                               for(int qq=0;qq<rtwUsers2.size();qq++)
                               {
                                   int inter=0;
                                   //χώρισε σε tokens και το retweet προς συγκριση και του χρηστη 2
                                   String str2 = rtwUsers2.get(qq);
                                   System.out.println("RT2= "+str2);
                                   StringTokenizer defaultTokenizer2 = new StringTokenizer(str2);
                                   ArrayList<String> tokens2 = new ArrayList();
                                   while (defaultTokenizer2.hasMoreTokens())
                                         { 
                                             
                                           tokens2.add(defaultTokenizer2.nextToken());
                                         }
                                 
                                   
                                   for(int w=0;w<tokens1.size();w++)
                                   {
                                       
                                       String word = tokens1.get(w);
                                       System.out.println("word = "+word);
                                       
                                       if(tokens2.contains(word))
                                       {
                                           System.out.println("exists");
                                           inter++;
                                       }
                                       
                                   }
                                   
                                   System.out.println("size1 = "+ tokens1.size());
                                   System.out.println("size2 = " + tokens2.size());
                                   System.out.println("inter = "+ inter);
                                  int allrt = tokens1.size() + tokens2.size() - inter;// όλα τα tags και των 2 χρηστών μαζι - τα κοινα μια φορα
                          
                                  double similarity = (double) inter/allrt;
                                   System.out.println("simirality = "+similarity);
                            
                                 similar.add(similarity);
                                 allRt++; 
                               }//τελειωνω με ολα τα rt του 2
                               
                               for(int e=0;e<similar.size();e++)
                               {
                                   simi.add(similar.get(e));
                               }
                               
                               
                               
                           }  //εδω τελειωνεις με ολα τα rt του 1
                  
                   
                   double sum=0;
                   double similarity=0;
                    for(int r=0;r<simi.size();r++)
                    {
                        sum=(sum+simi.get(r));
                       
                    }
                  similarity = sum/simi.size();
                 RetweetSimilarity.put(User2,similarity);
                  
             }
              RetweetsTable.put(User1,RetweetSimilarity);
            
        }
        
       
        
        
        
        ///////////////////////The 5th hash with the whole final similarity/////////////
        
        ArrayList<String> ListOfUsers = new ArrayList();
        
         for( DBObject tags : tagsCollection.find() )
              {
             //Για κάθε νέο αντικείμενο πάρε το όνομα του user
             String User = tags.get("user").toString();
             ListOfUsers.add(User); //φτιάξε τη λίστα με όλους τους χρήστες
              }
         
         
         
         
         
    HashMap<String,HashMap<String,Double>> FinalSimilarites = new HashMap(); //τελικές ομοιότητες για τον χρήστη user1 και κάθε άλλο χρήστη
           
         
                 
        //all users have utlist one hastag so i will take them from that hashmap
    for(int us=0;us<ListOfUsers.size();us++)
        {
            int me1=0; //δεν υπάρχει ο user στο ΜentionedTables
            int ur1=0; //δεν υπάρχει ο user στο UrlsTable
            int rt1=0; //δεν υπάρχει ο user στο RetweetsTable
            
             //σε πόσους πίνακες υπάρχουν
            
            String user1=ListOfUsers.get(us); //παίρνω τον πρώτο χρήστη
            
           
            
            //τότε πάρε τη λίστα με τους users και την ομοιότητα με αυτούς τους χρήστες για καθε αλλο πίνακα απο τους 4
            HashMap<String,Double> HashTags = new HashMap();
            HashTags=TagsTable.get(user1); //Γιατί οι χρήστες είναι από αυτό το hashmap άρα υπάρχει ο χρήστης μέσα σε αυτό
            
            //Eλέγχω αν ο πρώτος user ανήκει στα άλλα table
            
            if(MetionedUsersTable.containsKey(user1))
            {
                me1=1;
            }
            
            
            if(UrlsTable.containsKey(user1))
            {
                ur1=1;
                
            }
            
             
            if(RetweetsTable.containsKey(user1))
             {
               rt1=1;
               
             }
            
         
              HashMap<String,Double> FinalSimHelp = new HashMap();
              
        //οι χρήστες ειναι παλι απο το tagstable
        for(int us2=0;us2<ListOfUsers.size();us2++)
            {
                int numberOfExistance=0;
                 
                String user2 = ListOfUsers.get(us2);
                double similarity = HashTags.get(user2); //πάρε το similarity για κάθε άλλο χρήστη
                double SumOfUserSimilaritites = similarity;// το άθροισμα similarities απο καθε πινακα με καθε αλλο χρηστη 
                //εδώ όλοι με όλους έχουν ομοιότητα στο tagsTable 
                numberOfExistance++; //γιατί υπάρχουν και οι δυο στον πίνακα με τα tags
                
                if(me1==1)//αν χρήστης user1 ανήκει στον πίνακα metionUsersTable
                    {
                    
                         HashMap<String,Double> HashMeUsers = new HashMap();
                         HashMeUsers=MetionedUsersTable.get(user1); //πάρε το hash με τους αλλους users και τις ομοιότητές τους
                         
                        if(HashMeUsers.containsKey(user2))//δες αν βρεις μέσα στη λίστα 
                            {
                             
                             double similarityMU = HashMeUsers.get(user2);//πάρε το similarity για αυτόν τον user
                             SumOfUserSimilaritites=  SumOfUserSimilaritites + similarityMU; 
                             numberOfExistance++; //γιατί υπάρχουν και οι δυο στον πίνακα με τα με τους metioned users

                            } 
                    }
             
                if(ur1==1)
                {
                     HashMap<String,Double> HashUrls = new HashMap();
                     HashUrls=UrlsTable.get(user1);//πάρε το hash με τους αλλους users και τις ομοιότητές τους
                
                     if(HashUrls.containsKey(user2))
                        {
                            double similarityUr = HashUrls.get(user2);//πάρε το similarity για αυτόν τον user
                            SumOfUserSimilaritites=  SumOfUserSimilaritites + similarityUr;
                             numberOfExistance++; //γιατί υπάρχουν και οι δυο στον πίνακα με τα urls
                        }
                    
                }
            
                 if(rt1==1)
                {
                     HashMap<String,Double> HashRTweet = new HashMap();
                     HashRTweet=RetweetsTable.get(user1);//πάρε το hash με τους αλλους users και τις ομοιότητές τους
                
                     if(HashRTweet.containsKey(user2))
                        {
                            double similarityTw = HashRTweet.get(user2);//πάρε το similarity για αυτόν τον user
                            SumOfUserSimilaritites=  SumOfUserSimilaritites + similarityTw;
                             numberOfExistance++; //γιατί υπάρχουν και οι δυο στον πίνακα με τα retweets
                        }
                    
                }
                 
                 double FSimilarities = SumOfUserSimilaritites/numberOfExistance; //γτ 4 είναι οι ομοιότητες
                 FinalSimHelp.put(user2,FSimilarities);
                 //τελικές ομοιότητες για τον χρήστη user1 και κάθε άλλο χρήστη    
            }
            
          
         FinalSimilarites.put(user1, FinalSimHelp);
        }
       
    
    
    
    
    
    
    ///////////////////////////////FILES////////////////////////////////////////////////////////////////
        
        //θα τα γράψω σε αρχείο για να τα πάρει η κοπέλα να συνεχίσει με την εργασία
    
   
    List<String> l1 = new ArrayList<String>(TagsTable.keySet());
    List<String> l2 = new ArrayList<String>(UrlsTable.keySet());
    List<String> l3 = new ArrayList<String>(MetionedUsersTable.keySet());
    List<String> l4 = new ArrayList<String>(RetweetsTable.keySet());
    List<String> l5 = new ArrayList<String>(FinalSimilarites.keySet());
    
   
    
    ArrayList<String> Matrix1 = new ArrayList();
    for(int a=0;a<l1.size();a++)
    {
        String User1= l1.get(a);//παίρνω κάθε χρήστη
        
        HashMap<String,Double> us1 = new HashMap();
        us1=TagsTable.get(User1);//παίρνω το hashmap που αντιστοιχεί σε κάθε χρήστη
        
       
        List<String> l12= new ArrayList<String>(us1.keySet());//παίρνω όλους τους χρήστες2
        
      
        for(int a2=0;a2<l12.size();a2++)//για κάθε χρήστη 2 πάρε την ομοιότητά του με τον χρήστη 1
        {  
            
            String User2 = l12.get(a2);
            String sim = us1.get(User2).toString();
            Matrix1.add(User1);
            Matrix1.add(User2);
            Matrix1.add(sim);
            
        }
        
        
    }
    
    
      
      
    ArrayList<String> Matrix2 = new ArrayList();
    for(int a=0;a<l2.size();a++)
    {
        String User1= l2.get(a);//παίρνω κάθε χρήστη
       
        HashMap<String,Double> us2 = UrlsTable.get(User1);//παίρνω το hashmap που αντιστοιχεί σε κάθε χρήστη
        
        List<String> l22= new ArrayList<String>(us2.keySet());//παίρνω όλους τους χρήστες2
        
       
        for(int a2=0;a2<l22.size();a2++)//για κάθε χρήστη 2 πάρε την ομοιότητά του με τον χρήστη 1
        {  
            
            String User2 = l22.get(a2);
            String sim = us2.get(User2).toString();
            Matrix2.add(User1);
            System.out.println(" user1 = "+User1);
            Matrix2.add(User2);
             System.out.println(" user2 = "+User2);
            Matrix2.add(sim);
             System.out.println(" similarity= "+sim);
           
            
        }
        
        
    }
    
    
    ArrayList<String> Matrix3 = new ArrayList();
    for(int a=0;a<l3.size();a++)// για καθε χρηστη 1
    {
        
        String User1= l3.get(a);//παίρνω κάθε χρήστη
       System.out.println("Userrrrr1 = " + User1);
       
        HashMap<String,Double> us3 = MetionedUsersTable.get(User1);//παίρνω το hashmap που αντιστοιχεί σε κάθε χρήστη
        
        List<String> l13= new ArrayList<String>(us3.keySet());//παίρνω όλους τους χρήστες2
        
        
        
        
        
        
       
        for(int a2=0;a2<l13.size();a2++)//για κάθε χρήστη 2 πάρε την ομοιότητά του με τον χρήστη 1
        {  
             System.out.println("Userrrrr1 = " + User1);
             
            String User2 = l13.get(a2);
             System.out.println("Userrrrr2 = " + User2);
            String sim = us3.get(User2).toString();
            Matrix3.add(User1);
            Matrix3.add(User2);
            Matrix3.add(sim);
            
           
           
        }
        
    }
    
    
    ArrayList<String> Matrix4 = new ArrayList();
    for(int a=0;a<l4.size();a++)
    {
        String User1= l4.get(a);//παίρνω κάθε χρήστη
       
        HashMap<String,Double> us4 = RetweetsTable.get(User1);//παίρνω το hashmap που αντιστοιχεί σε κάθε χρήστη
        List<String> l14= new ArrayList<String>(us4.keySet());//παίρνω όλους τους χρήστες2
        
       
        for(int a2=0;a2<l14.size();a2++)//για κάθε χρήστη 2 πάρε την ομοιότητά του με τον χρήστη 1
        {  
            
            String User2 = l14.get(a2);
            String sim = us4.get(User2).toString();
            Matrix4.add(User1);
            Matrix4.add(User2);
            Matrix4.add(sim);
           
           
        }
        
        
    }
    ArrayList<String> Matrix5 = new ArrayList();
    for(int a=0;a<l5.size();a++)
    {
        String User1= l5.get(a);//παίρνω κάθε χρήστη
       
        HashMap<String,Double> us5 = FinalSimilarites.get(User1);//παίρνω το hashmap που αντιστοιχεί σε κάθε χρήστη
        List<String> l15= new ArrayList<String>(us5.keySet());//παίρνω όλους τους χρήστες2
        
     
        for(int a2=0;a2<l15.size();a2++)//για κάθε χρήστη 2 πάρε την ομοιότητά του με τον χρήστη 1
        {  
            
            String User2 = l15.get(a2);
            String sim = us5.get(User2).toString();
            Matrix5.add(User1);
            Matrix5.add(User2);
            Matrix5.add(sim);
           
            
        }
        
        
    }
      
     
     
     
     
   
    try
    {
        BufferedWriter writer1 = new BufferedWriter( new FileWriter(".\\HashtagsSimilarities.txt"));
        writer1.write("    User 1" + "      " + "            User 2" + "                " + "    Similarity\n");
        int s=0;
         while(s<Matrix1.size())
         {
             writer1.write(Matrix1.get(s) + "          "+ Matrix1.get(s+1)+ "              "+Matrix1.get(s+2)+"\n");
             s=s+3;
             
         }
        writer1.close();
        
        BufferedWriter writer2 = new BufferedWriter( new FileWriter(".\\UrlsSimilarities.txt"));
        writer2.write("User 1" + "    " + " User 2" + "    " + " Similarity\n");
        
       int s2=0;
        while(s2<Matrix2.size())
         {
             writer2.write(Matrix2.get(s2) + "          "+ Matrix2.get(s2+1)+ "              "+Matrix2.get(s2+2)+"\n");
             s2=s2+3;
             
         }
        writer2.close();
        
        BufferedWriter writer3 = new BufferedWriter( new FileWriter(".\\MetionedUsersSimilarities.txt"));
        writer3.write("User 1" + "    " + " User 2" + "    " + " Similarity\n");
        int s3=0;
        while(s3<Matrix3.size())
         {
             writer3.write(Matrix3.get(s3) + "          "+ Matrix3.get(s3+1)+ "              "+Matrix3.get(s3+2)+"\n");
             s3=s3+3;
             
         }
        writer3.close();
        
        BufferedWriter writer4 = new BufferedWriter( new FileWriter(".\\ReTweetsSimilarities.txt"));
        writer4.write("User 1" + "    " + " User 2" + "    " + " Similarity\n");
       int s4=0;
        while(s4<Matrix4.size())
         {
             writer4.write(Matrix4.get(s4) + "          "+ Matrix4.get(s4+1)+ "              "+Matrix4.get(s4+2)+"\n");
             s4=s4+3;
             
         }
        writer4.close();
        
        BufferedWriter writer5 = new BufferedWriter( new FileWriter(".\\FinalSimilarities.txt"));
        writer5.write("       User 1  " + "         " + "       User 2" + "         " + " Similarity\n");
        int s5=0;
        while(s5<Matrix5.size())
         {
             writer5.write(Matrix5.get(s5) + "          "+ Matrix5.get(s5+1)+ "              "+Matrix5.get(s5+2)+"\n");
             s5=s5+3;
             
         }
        writer5.close();
        
        
        
    }catch(IOException e){
        e.printStackTrace();
    }

   
   
        
        
        
        
    } catch (MongoException e) {
	e.printStackTrace();
    }


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
