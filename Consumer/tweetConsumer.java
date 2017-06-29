/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;

public class tweetConsumer implements Serializable{

    String msg;
    double latitude;
    double longitude;
    String sentiment;
    Date timestamp = Calendar.getInstance().getTime();
    String location;

    public String getMsg() {
        return msg;
    }
    
    public void setTimestamp(){
//        timestamp = new Timestamp(System.currentTimeMillis()).toString();
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public String getSentiment() {
        return sentiment;
    }

    public void setSentiment(String sentiment) {
        this.sentiment = sentiment;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    @Override
    public String toString() {
        return "tweetConsumer{" + "msg=" + msg + ", latitude=" + latitude + ", longitude=" + longitude + ", sentiment=" + sentiment + ", timestamp=" + timestamp + ", location=" +'}';
    }
    
    

}