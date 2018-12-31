package net.sf.exdev.samplers.memcached;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.Interruptible;
import org.apache.jmeter.samplers.SampleResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemcachedJavaSampler extends AbstractJavaSamplerClient implements Interruptible {

	private static final String HOST = "Host";
	private static final String PORT = "Port";
	private static final String INPUT_FILE = "Input File";
	private static final Logger LOG = LoggerFactory.getLogger(MemcachedJavaSampler.class);

	private volatile String host = "localhost";
	private volatile int port = 11211;
	private volatile File inputFile = new File(System.getProperty("user.home") + File.separator);
	private volatile MemCachedMediator memcachedMediator = null;

	// https://stackoverflow.com/questions/106179/regular-expression-to-match-dns-hostname-or-ip-address
	private static final String PATTERN_IP = "^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$";
	private static final String PATTERN_HOSTNAME = "^(([a-zA-Z]|[a-zA-Z][a-zA-Z0-9-]*[a-zA-Z0-9]).)*([A-Za-z]|[A-Za-z][A-Za-z0-9-]*[A-Za-z0-9])$";

	@Override
	public Arguments getDefaultParameters() {
		Arguments params = new Arguments();
		params.addArgument(HOST, host);
		params.addArgument(PORT, String.valueOf(port));
		params.addArgument(INPUT_FILE, inputFile.toString());
		return params;
	}

	@Override
	public void setupTest(JavaSamplerContext context) {
		host = context.getParameter(HOST);
		if (host == null || host.trim().isEmpty() && (!host.matches(PATTERN_IP) || !host.matches(PATTERN_HOSTNAME))) {
			throw new IllegalArgumentException("Host cannot be empty, default: localhost");
		}

		port = Integer.parseInt(context.getParameter(PORT));
		if (0 > port && port > 65535) {
			throw new IllegalArgumentException("Port value must be between 1 and 65535, default: 11211");
		}

		inputFile = new File(context.getParameter(INPUT_FILE));
		if (!inputFile.exists() || !inputFile.isFile()) {
			throw new IllegalArgumentException(
					String.format("%s does not exist or is not a file", inputFile.toString()));
		}

		memcachedMediator = new MemCachedMediator(host, port);
		LOG.info("MemCached Mediator: {}", memcachedMediator);
	}

	@SuppressWarnings("unchecked")
	public SampleResult runTest(JavaSamplerContext context) {
		Map<String, Object> output = null;
		SampleResult results = new SampleResult();
		results.setSampleLabel("MemCached");
		results.setContentType(System.getProperty("file.encoding"));
		results.setDataType(SampleResult.TEXT);

		try {
			results.sampleStart();
			List<String> lines = memcachedMediator.getFileContent(inputFile);
			output = memcachedMediator.parse(lines);
			String bodyContent = lines.toString().replace("[", "").replace("]", "").replaceAll(", ", "\n");
			results.setBodySize((long) bodyContent.getBytes().length);
			results.setSamplerData(bodyContent);
			results.setResponseData(output.toString().replace("{", "").replace("}", "").replaceAll(", ", "\n"),
									System.getProperty("file.encoding"));
			results.setResponseMessage(output.toString());
			results.setResponseCode("200");
			results.setResponseOK();
			results.setResponseMessageOK();
			results.setResponseCodeOK();
			results.setSuccessful(true);

		} catch (Exception ex) {
			results.setResponseCode("500");
			LOG.error(ex.getMessage(), ex);

			if (output != null) {
				results.setErrorCount((int) output.get("errors"));
				results.setResponseMessage(((List<String>) output.get("error-messages")).toString());

			} else {
				LOG.warn("Using fallback counter, something seems wrong");
				results.setErrorCount(results.getErrorCount() + 1);
				results.setResponseMessage(ex.getMessage());
			}

			results.setSuccessful(false);

		} finally {
			results.sampleEnd();

			if (memcachedMediator != null) {
				try {
					memcachedMediator.close();

				} catch (IOException ioEx) {
					LOG.error("Closing connection the hard way...", ioEx);

				} finally {
					memcachedMediator = null;
				}
			}
		}

		return results;
	}

	@Override
	public boolean interrupt() {
		if (memcachedMediator != null) {
			try {
				LOG.info("Trying to close connection with MemCached");
				memcachedMediator.close();

			} catch (IOException ioEx) {
				LOG.error("Unable to shutdown MemCached Mediator", ioEx);

			} finally {
				LOG.info("Forcing MemCached Mediator to null");
				memcachedMediator = null;
			}
		}

		return (memcachedMediator != null);
	}

}
