package com.cor.cep.handler;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.StandardLocation;
import javax.tools.ToolProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.cor.cep.event.TemperatureEvent;
import com.cor.cep.subscriber.StatementSubscriber;
import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.compiler.client.EPCompiler;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.EPDeployException;
import com.espertech.esper.runtime.client.EPDeployment;
import com.espertech.esper.runtime.client.EPRuntime;
import com.espertech.esper.runtime.client.EPRuntimeDestroyedException;
import com.espertech.esper.runtime.client.EPRuntimeProvider;
import com.espertech.esper.runtime.client.EPStatement;

/**
 * This class handles incoming Temperature Events. It processes them through the EPService, to which
 * it has attached the 3 queries.
 */
@Component
@Scope(value = "singleton")
public class TemperatureEventHandler implements InitializingBean{

    /** Logger */
    private static Logger LOG = LoggerFactory.getLogger(TemperatureEventHandler.class);

    /** Esper service */
    private Configuration config;
	private EPRuntime epRuntime;
	private EPCompiler compiler;
	private CompilerArguments args;
	
    private EPStatement criticalEventStatement;
    private EPStatement warningEventStatement;
    private EPStatement monitorEventStatement;

    @Autowired
    @Qualifier("criticalEventSubscriber")
    private StatementSubscriber criticalEventSubscriber;

    @Autowired
    @Qualifier("warningEventSubscriber")
    private StatementSubscriber warningEventSubscriber;

    @Autowired
    @Qualifier("monitorEventSubscriber")
    private StatementSubscriber monitorEventSubscriber;

    /**
     * Configure Esper Statement(s).
     */
    public void initService() throws Exception {

        LOG.debug("Initializing Servcie ..");
        config = new Configuration();
        // config.getCommon().addEventType(TemperatureEvent.class);
        // example for using addEventTypeAutoName
        // addEventTypeAutoName not works like before. seems like just path-prefix
        config.getCommon().addEventTypeAutoName(TemperatureEvent.class.getPackage().getName());
        Collection<Class<?>> classes = getClasses(TemperatureEvent.class.getPackage().getName());
        for (Class<?> clazz : classes) {
        	// config.getCommon().addEventType(clazz);	
        	config.getCommon().addEventType(clazz.getSimpleName(), clazz.getSimpleName());
		}
		config.getCompiler().getByteCode().setAllowSubscriber(true);
		epRuntime = EPRuntimeProvider.getDefaultRuntime(config);
		compiler = EPCompilerProvider.getCompiler();
		args = new CompilerArguments(config);

        createCriticalTemperatureCheckExpression();
        createWarningTemperatureCheckExpression();
        createTemperatureMonitorExpression();

    }
    
    /**
     * https://stackoverflow.com/questions/520328/can-you-find-all-classes-in-a-package-using-reflection#32828953 
     */
    public static Collection<Class<?>> getClasses(final String pack) throws Exception {
        final StandardJavaFileManager fileManager = ToolProvider.getSystemJavaCompiler().getStandardFileManager(null, null, null);
        return StreamSupport.stream(fileManager.list(StandardLocation.CLASS_PATH, pack, Collections.singleton(JavaFileObject.Kind.CLASS), false).spliterator(), false)
                .map(javaFileObject -> {
                    try {
                        final String[] split = javaFileObject.getName()
                                .replace(".class", "")
                                .replace(")", "")
                                .split(Pattern.quote(File.separator));

                        final String fullClassName = pack + "." + split[split.length - 1];
                        return Class.forName(fullClassName);
                    } catch (ClassNotFoundException e) {
                        throw new RuntimeException(e);
                    }

                })
                .collect(Collectors.toCollection(ArrayList::new));
    }

    /**
     * EPL to check for a sudden critical rise across 4 events, where the last event is 1.5x greater
     * than the first event. This is checking for a sudden, sustained escalating rise in the
     * temperature
     * @throws EPCompileException 
     * @throws EPDeployException 
     * @throws EPRuntimeDestroyedException 
     */
    private void createCriticalTemperatureCheckExpression() throws EPCompileException, EPRuntimeDestroyedException, EPDeployException {
        
        LOG.debug("create Critical Temperature Check Expression");
        EPCompiled compiled = compiler.compile(buildStatementWithName(criticalEventSubscriber), args);
		EPDeployment epDeployment = epRuntime.getDeploymentService().deploy(compiled);
		criticalEventStatement = epRuntime.getDeploymentService().getStatement(epDeployment.getDeploymentId(),
				criticalEventSubscriber.getStatementName());
		criticalEventStatement.setSubscriber(criticalEventSubscriber);
    }

    /**
     * EPL to check for 2 consecutive Temperature events over the threshold - if matched, will alert
     * listener.
     * @throws EPCompileException 
     * @throws EPDeployException 
     * @throws EPRuntimeDestroyedException 
     */
    private void createWarningTemperatureCheckExpression() throws EPCompileException, EPRuntimeDestroyedException, EPDeployException {

        LOG.debug("create Warning Temperature Check Expression");
        EPCompiled compiled = compiler.compile(buildStatementWithName(warningEventSubscriber), args);
		EPDeployment epDeployment = epRuntime.getDeploymentService().deploy(compiled);
		warningEventStatement = epRuntime.getDeploymentService().getStatement(epDeployment.getDeploymentId(),
				warningEventSubscriber.getStatementName());
		warningEventStatement.setSubscriber(warningEventSubscriber);
    }

    /**
     * EPL to monitor the average temperature every 10 seconds. Will call listener on every event.
     * @throws EPCompileException 
     * @throws EPDeployException 
     * @throws EPRuntimeDestroyedException 
     */
    private void createTemperatureMonitorExpression() throws EPCompileException, EPRuntimeDestroyedException, EPDeployException {

        LOG.debug("create Timed Average Monitor");
        EPCompiled compiled = compiler.compile(buildStatementWithName(monitorEventSubscriber), args);
		EPDeployment epDeployment = epRuntime.getDeploymentService().deploy(compiled);
		monitorEventStatement = epRuntime.getDeploymentService().getStatement(epDeployment.getDeploymentId(),
				monitorEventSubscriber.getStatementName());
        monitorEventStatement.setSubscriber(monitorEventSubscriber);
    }
    
    private String buildStatementWithName(StatementSubscriber statementSubscriber) {
		return String.join("", "@name('", statementSubscriber.getStatementName(), "') ",
				statementSubscriber.getStatement());
	}

    /**
     * Handle the incoming TemperatureEvent.
     */
    public void handle(TemperatureEvent event) {

        LOG.debug(event.toString());
        epRuntime.getEventService().sendEventBean(event, TemperatureEvent.class.getSimpleName());

    }

    @Override
    public void afterPropertiesSet() {
        
        LOG.debug("Configuring..");
        try {
			initService();
		} catch (Exception e) {
			LOG.error("Failed to configure.", e);
		}
    }
}
