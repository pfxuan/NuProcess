package com.zaxxer.nuprocess.streams;

import com.zaxxer.nuprocess.NuProcessBuilder;
import java.nio.ByteBuffer;
import org.reactivestreams.Publisher;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;
import org.testng.SkipException;

public class TckFiniteStdoutPublisherTest extends PublisherVerification<ByteBuffer> {

  private static final long NUMBER_OF_MESSAGES = 5L;
  private static final long DEFAULT_TIMEOUT = 300L;
  private static final long DEFAULT_GC_TIMEOUT = 1000L;
  private String command;

  public TckFiniteStdoutPublisherTest() {
    super(new TestEnvironment(DEFAULT_TIMEOUT), DEFAULT_GC_TIMEOUT);
    command = "cat";
    if (System.getProperty("os.name").toLowerCase().contains("win")) {
      command = "src\\test\\java\\com\\zaxxer\\nuprocess\\cat.exe";
    }
  }

  @Override
  public Publisher<ByteBuffer> createPublisher(long elements) {
    NuProcessBuilder builder = new NuProcessBuilder(command, "src/test/resources/chunk.txt");
    NuStreamProcessBuilder streamBuilder = new NuStreamProcessBuilder(builder);
    NuStreamProcess process = streamBuilder.start(NUMBER_OF_MESSAGES);

    return process.getStdoutPublisher();
  }

  @Override
  public Publisher<ByteBuffer> createFailedPublisher() {
    throw new SkipException("Not implemented");

  }
}
