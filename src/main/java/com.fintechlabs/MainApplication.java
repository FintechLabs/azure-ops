package com.fintechlabs;

import java.io.FileInputStream;
import java.util.Properties;

public class MainApplication {

    public static void main(String... args) {
        try {
            System.out.println("Starting Azure Ops Application...");
            ClassLoader classLoader = ClassLoader.getSystemClassLoader();
            Properties appProps = new Properties();
            appProps.load(new FileInputStream(classLoader.getResource("application.properties").getFile()));
            final String SOURCE_STORAGE_ACCOUNT_CONNECTION_STRING = appProps.getProperty("source.storage.account.connection.string");
            final String DEST_STORAGE_ACCOUNT_CONNECTION_STRING = appProps.getProperty("destination.storage.account.connection.string");

//            AzureStorage.listSourceContainers(SOURCE_STORAGE_ACCOUNT_CONNECTION_STRING);
            AzureStorage.copyContainer(SOURCE_STORAGE_ACCOUNT_CONNECTION_STRING, DEST_STORAGE_ACCOUNT_CONNECTION_STRING);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
