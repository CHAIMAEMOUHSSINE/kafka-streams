package net.chaimae.kafkaspringcloudstream.events;

public record PageEvent(String page, String user, java.util.Date date, long timestamp) {
}
