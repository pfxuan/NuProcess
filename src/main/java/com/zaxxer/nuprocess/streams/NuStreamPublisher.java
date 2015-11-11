/*
 * Copyright (C) 2015 Brett Wooldridge
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zaxxer.nuprocess.streams;

import java.nio.ByteBuffer;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.zaxxer.nuprocess.NuProcess.Stream;
import com.zaxxer.nuprocess.streams.NuStreamProcessBuilder.BridgeProcessHandler;

public class NuStreamPublisher implements Publisher<ByteBuffer>
{
   private final BridgeProcessHandler processHandler;
   private Stream stream;
   private long numOfElements;

   NuStreamPublisher(final BridgeProcessHandler processHandler, final Stream stream)
   {
      this(processHandler, stream, Long.MAX_VALUE);
   }

   NuStreamPublisher(final BridgeProcessHandler processHandler, final Stream stream, long numOfElements)
   {
      this.processHandler = processHandler;
      this.stream = stream;
      this.numOfElements = numOfElements;
   }

   @Override
   public void subscribe(final Subscriber<? super ByteBuffer> subscriber)
   {
      if (subscriber == null) {
         throw new NullPointerException("Subscriber cannot be null");
      }

      processHandler.setSubscriber(stream, subscriber);

      if (numOfElements == Long.MAX_VALUE) {
         // infinite stream
         subscriber.onSubscribe(new NuStreamSubscription());
      } else {
         // finite stream
         subscriber.onSubscribe(new NuStreamSubscription2(subscriber));
      }
   }

   class NuStreamSubscription implements Subscription
   {
      @Override
      public void cancel()
      {
         if (stream != null) {
            final Stream s = stream;
            stream = null;
            processHandler.cancel(s);
         }
      }

      @Override
      public void request(long n)
      {
         if (n < 1) {
            throw new IllegalArgumentException("Subscription.request() value cannot be less than 1");
         }

         if (stream != null) {
            processHandler.request(stream, n);
         }
      }
   }

   class NuStreamSubscription2 implements Subscription
   {
      private Subscriber subscriber;

      public NuStreamSubscription2(Subscriber subscriber) {
         this.subscriber = subscriber;
      }

      @Override
      public void cancel()
      {
         if (stream != null) {
            final Stream s = stream;
            stream = null;
            processHandler.cancel(s);
         }
      }

      @Override
      public void request(long n)
      {
         if (n < 1) {
            throw new IllegalArgumentException("Subscription.request() value cannot be less than 1");
         }

         if (stream != null) {
            for (int i = 0; i < n; i++) {
               if (numOfElements > 0) {
                  processHandler.request(stream, 1);
                  //subscriber.onNext(ByteBuffer buffer);
                  numOfElements--;
               } else {
                  subscriber.onComplete();
               }
            }
         }
      }
   }


}
