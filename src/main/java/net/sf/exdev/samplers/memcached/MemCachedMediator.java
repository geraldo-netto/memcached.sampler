package net.sf.exdev.samplers.memcached;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.rubyeye.xmemcached.MemcachedClient;
import net.rubyeye.xmemcached.XMemcachedClient;
import net.rubyeye.xmemcached.exception.MemcachedException;

public class MemCachedMediator {

	private volatile MemcachedClient client = null;
	private static final Logger LOG = LoggerFactory.getLogger(MemCachedMediator.class);

	public MemCachedMediator(String host, int port) {
		try {
			client = new XMemcachedClient(host, port);
			LOG.info("MemCached configured: {}:{}", host, port);

		} catch (IOException ioEx) {
			throw new IllegalStateException("Unable to start MemCached client", ioEx);
		}
	}

	public boolean add(String key, String value, int ttl)
			throws TimeoutException, InterruptedException, MemcachedException {
		return client.add(key, ttl, value);
	}

	public boolean set(String key, String value, int ttl)
			throws TimeoutException, InterruptedException, MemcachedException {
		return client.set(key, ttl, value);
	}

	public boolean append(String key, String value) throws TimeoutException, InterruptedException, MemcachedException {
		return client.append(key, value);
	}

	public boolean prepend(String key, String value) throws TimeoutException, InterruptedException, MemcachedException {
		return client.prepend(key, value);
	}

	public String get(String key) throws TimeoutException, InterruptedException, MemcachedException {
		return client.get(key);
	}

	public boolean delete(String key) throws TimeoutException, InterruptedException, MemcachedException {
		return client.delete(key);
	}

	public boolean flush() throws TimeoutException, InterruptedException, MemcachedException {
		client.flushAll();
		return true;
	}

	public List<String> getFileContent(File file) throws IOException {
		List<String> lines = new ArrayList<>();
		try (BufferedReader buffer = new BufferedReader(new FileReader(file))) {
			String line;
			while ((line = buffer.readLine()) != null) {
				lines.add(line);
			}
		}

		return lines;
	}

	public Map<String, Object> parse(List<String> lines)
			throws TimeoutException, InterruptedException, MemcachedException {
		int total = 0;
		int error = 0;
		Map<String, Object> result = new HashMap<>();
		List<String> errorMessages = new ArrayList<>();

		for (String line : lines) {
			String[] command = line.trim().split("  ");
			switch (command[0].toLowerCase()) {
			case "add":
				add(command[1], command[2], Integer.parseInt(command[3]));
				break;

			case "set":
				set(command[1], command[2], Integer.parseInt(command[3]));
				break;

			case "append":
				append(command[1], command[2]);
				break;

			case "prepend":
				prepend(command[1], command[2]);
				break;

			case "get":
				get(command[1]);
				break;

			case "delete":
				delete(command[1]);
				break;

			case "flush":
				flush();
				break;

			default:
				error++;
				errorMessages.add(String.format("Input line %s is malformed", line));
				LOG.error("Input line {} is malformed", line);
			}

			total++;
		}

		result.put("total", total);
		result.put("success", total - error);
		result.put("errors", error);
		result.put("error-messages", errorMessages);

		return result;
	}

	public boolean close() throws IOException {
		client.shutdown();
		return true;
	}

}
