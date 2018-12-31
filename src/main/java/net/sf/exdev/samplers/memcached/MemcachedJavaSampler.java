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

	private static final Logger LOG = LoggerFactory.getLogger(MemcachedJavaSampler.class);

	private static final String HOST = "Host";
	private static final String PORT = "Port";
	private static final String INPUT_FILE = "Input File";

	private String host = "localhost";
	private int port = 11211;
	private File inputFile = new File(System.getProperty("user.home"));
	private MemCachedMediator memcachedMediator = null;

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
		setupValues(context);
	}

	private void setupValues(JavaSamplerContext context) {
		host = context.getParameter(HOST);
		if (host == null || host.trim().isEmpty()) {
			throw new IllegalArgumentException("Host cannot be empty");
		}

		port = Integer.parseInt(context.getParameter(PORT));
		if (port < 1 || port > 65535) {
			throw new IllegalArgumentException("Port value must be between 1 and 65535");
		}

		inputFile = new File(context.getParameter(INPUT_FILE));
		if (!inputFile.exists() || !inputFile.isFile()) {
			throw new IllegalArgumentException(
					String.format("%s does not exist or is not a file", inputFile.toString()));
		}

		try {
			memcachedMediator = new MemCachedMediator(host, port, inputFile);

		} catch (IOException ioEx) {
			LOG.error(ioEx.getMessage());
		}
	}

	@SuppressWarnings("unchecked")
	public SampleResult runTest(JavaSamplerContext context) {
		Map<String, Object> output = null;

		SampleResult results = new SampleResult();
		results.setSampleLabel("MemCached");
		results.setContentType(System.getProperty("file.encoding"));
		results.setDataType(SampleResult.TEXT);

		try {
			if (memcachedMediator == null) {
				throw new IOException("Unable to instantiate MemCached client, aborting");
			}

			results.sampleStart();
			List<String> lines = memcachedMediator.getFileContent(inputFile);
			results.setSamplerData(lines.toString());
			output = memcachedMediator.parse(lines);
			results.setResponseData(output.toString(), System.getProperty("file.encoding"));

			results.setResponseMessage(output.toString());
			results.setResponseOK();
			results.setResponseMessageOK();
			results.setResponseCodeOK();
			results.setSuccessful(true);

		} catch (Exception ex) {
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

				} catch (IOException ex) {
					LOG.error("Closing connection the hard way...", ex);
					memcachedMediator = null;
				}
			}
		}

		return results;
	}

	public boolean interrupt() {
		return false;
	}

}
