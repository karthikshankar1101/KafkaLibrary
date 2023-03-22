package org.example;

import java.util.concurrent.ExecutionException;

public class Main {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

       Kafka kafka = new Kafka("Karthik",4,"Hello Harsha","key-1");
    }
}