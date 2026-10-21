package net.chaimae.kafkaspringcloudstream.events;

public record PageEvent(String page, String user, long timestamp) {
}
