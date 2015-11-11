package com.zaxxer.nuprocess.streams;

import java.nio.ByteBuffer;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.zip.Adler32;

import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.zaxxer.nuprocess.NuProcessBuilder;

public class CatStreamTest
{
   private String command;

   @Before
   public void setup()
   {
      command = "cat";
      if (System.getProperty("os.name").toLowerCase().contains("win")) {
         command = "src\\test\\java\\com\\zaxxer\\nuprocess\\cat.exe";
      }
   }

   @Test
   public void slowRead() throws InterruptedException
   {
      class NaiveSubscriber implements Subscriber<ByteBuffer>
      {
         private Timer timer;
         private Subscription subscription;
         private Adler32 readAdler32;

         NaiveSubscriber()
         {
            timer = new Timer(true);
            readAdler32 = new Adler32();
         }

         @Override
         public void onComplete()
         {
            timer.cancel();
            System.err.printf("Final Adler32: %d\n", readAdler32.getValue());
         }
         
         @Override
         public void onError(Throwable t)
         {
         }
         
         @Override
         public void onNext(ByteBuffer buffer)
         {
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            readAdler32.update(bytes);
            System.err.printf("Adler32: %d\n", readAdler32.getValue());

            timer.schedule(new TimerTask() {
               public void run()
               {
                  subscription.request(1);
               }
            }, TimeUnit.SECONDS.toMillis(5));            
         }
         
         @Override
         public void onSubscribe(Subscription sub)
         {
            subscription = sub;
            timer.schedule(new TimerTask() {
               public void run()
               {
                  subscription.request(1);
               }
            }, 0);
         }
      }

      NuProcessBuilder builder = new NuProcessBuilder(command, "src/test/resources/chunk.txt");
      NuStreamProcessBuilder streamBuilder = new NuStreamProcessBuilder(builder);
      NuStreamProcess process = streamBuilder.start();

      NuStreamPublisher stdoutPublisher = process.getStdoutPublisher();

      NaiveSubscriber subscriber = new NaiveSubscriber();
      stdoutPublisher.subscribe(subscriber);

      process.waitFor(0, TimeUnit.SECONDS); // wait until the process exists
   }
}
