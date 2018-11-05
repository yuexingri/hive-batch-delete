package com.hive.cli;

public class HiveTools {
    public static void main(String[] args) {
        try {
            HiveClient hiveClient = new HiveClient();
            hiveClient.dropAllTables();
        } catch (Exception e) {
            System.out.println(e);
            //do nothing
        }
    }
}
