package com.relentless;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.*;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterToKinesis {
	static KinesisAsyncClient kinesis = KinesisAsyncClient.builder().build();

	public static void run(String consumerKey, String consumerSecret, String token, String secret, String streamName)
			throws InterruptedException, UnsupportedEncodingException {
		BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);
		StatusesSampleEndpoint endpoint = new StatusesSampleEndpoint();
		endpoint.stallWarnings(false);
		Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);
		BasicClient client = new ClientBuilder().name("sampleExampleClient")
				.hosts(Constants.STREAM_HOST)
				.endpoint(endpoint)
				.authentication(auth)
				.processor(new StringDelimitedProcessor(queue))
				.build();
		client.connect();
		while (true) {
			if (client.isDone()) {
				System.out.println("Client connection closed unexpectedly: " +
						client.getExitEvent().getMessage());
				break;
			} else {
				String msg = queue.poll(5, TimeUnit.SECONDS);
				if (msg == null) {
					System.err.println("Did not receive a message in 5 seconds. Is something wrong?");
				} else {
					System.out.print(msg);
					ingest(streamName, msg);
				}
			}

		}
		client.stop();
	}

	private static void ingest(String streamName, String msg) throws UnsupportedEncodingException {
		String partitionKey = UUID.randomUUID().toString();
		ByteBuffer data = ByteBuffer.wrap(msg.getBytes("UTF-8"));
		SdkBytes msgData = SdkBytes.fromByteArray(data.array());
		PutRecordRequest putRecordRequest = PutRecordRequest.builder()
				.streamName(streamName)
				.partitionKey(partitionKey)
				.data(msgData)
				.build();
		CompletableFuture<PutRecordResponse> putRecord = kinesis.putRecord(putRecordRequest);
		putRecord.whenComplete((resp, err) -> {
			if (err != null)
				err.printStackTrace();
			else if (data != null) {
				String sequenceNumber = resp.sequenceNumber();
				//System.out.println(String.format("Record [%s] ingested", sequenceNumber));
			}
		});
	}

	public static String requireSecret(String secretName) {
		String getenv = System.getenv(secretName);
		if (getenv == null)
			throw new RuntimeException(String.format("Required environment variable [%s] not found.", secretName));
		else
			return getenv;
	}

	public static void main(String[] args) {
		try {
			String consumerKey = requireSecret("CONSUMER_KEY");
			String consumerSecret = requireSecret("CONSUMER_SECRET");
			String token = requireSecret("APP_TOKEN");
			String secret = requireSecret("APP_SECRET");
			String streamName = requireSecret("KINESIS_STREAM_NAME");
			TwitterToKinesis.run(consumerKey, consumerSecret, token, secret, streamName);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("Exiting");
	}
}