package com.kafka;

//TestProducer.java
import org.apache.kafka.clients.producer.*;
import java.util.Arrays;
import java.util.Properties;
import java.lang.System;
import java.io.*;
import java.lang.Runtime;
import java.io.PrintWriter;

public class TestProducer {
  public static void main(String[] args) {
      try {
          PrintWriter caWriter = new PrintWriter("/tmp/ca.pem", "UTF-8");
          String ca = System.getenv("CLOUDKARAFKA_CA");
          caWriter.println(ca);
          caWriter.close();

          PrintWriter certWriter = new PrintWriter("/tmp/cert.pem", "UTF-8");
          String cert = System.getenv("CLOUDKARAFKA_CERT");
          certWriter.println(cert);
          certWriter.close();

          PrintWriter keyWriter = new PrintWriter("/tmp/key.pem", "UTF-8");
          String privateKey = System.getenv("CLOUDKARAFKA_PRIVATE_KEY");
          keyWriter.println(privateKey);
          keyWriter.close();

          Runtime r = Runtime.getRuntime();
          Process p = r.exec("openssl pkcs12 -export -password pass:test1234 -out /tmp/store.pkcs12 -inkey /tmp/key.pem -certfile /tmp/ca.pem -in /tmp/cert.pem -caname 'CA Root' -name client");
          p.waitFor();

          p = r.exec("keytool -importkeystore -noprompt -srckeystore /tmp/store.pkcs12 -destkeystore /tmp/keystore.jks -srcstoretype pkcs12 -srcstorepass test1234 -srckeypass test1234 -destkeypass test1234 -deststorepass test1234 -alias client");
          p.waitFor();

          p = r.exec("keytool -noprompt -keystore /tmp/truststore.jks -alias CARoot -import -file /tmp/ca.pem -storepass test1234");
          p.waitFor();

          String brokers = System.getenv("CLOUDKARAFKA_BROKERS");
          String topicPrefix = System.getenv("CLOUDKARAFKA_TOPIC_PREFIX");

          Properties props = new Properties();
          props.put("bootstrap.servers", brokers);
          props.put("group.id", "test");
          props.put("enable.auto.commit", "true");
          props.put("auto.commit.interval.ms", "1000");
          props.put("session.timeout.ms", "30000");
          props.put("key.serializer", org.apache.kafka.common.serialization.StringSerializer.class.getName());
          props.put("value.serializer", org.apache.kafka.common.serialization.StringSerializer.class.getName());
          props.put("security.protocol", "SSL");
          props.put("ssl.truststore.location", "/tmp/truststore.jks");
          props.put("ssl.truststore.password", "test1234");
          props.put("ssl.keystore.location", "/tmp/keystore.jks");
          props.put("ssl.keystore.password", "test1234");
          props.put("ssl.keypassword", "test1234");

          KafkaProducer<Object,Object> producer = new KafkaProducer<>(props);
          for(int i = 0; i < 100; i++)
              producer.send(new ProducerRecord(topicPrefix + "default", Integer.toString(i), Integer.toString(i)));

          producer.close();
      } catch (FileNotFoundException e) {
          System.out.println(e);
      } catch (UnsupportedEncodingException e) {
          System.out.println(e);
      } catch (IOException e) {
          System.out.println(e);
      } catch (InterruptedException e) {
          System.out.println(e);
      }
  }
}