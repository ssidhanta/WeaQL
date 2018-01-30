package weaql.common.util;


import weaql.common.nodes.NodeConfig;
import weaql.common.nodes.Role;
import weaql.common.util.exception.ConfigurationLoadException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import weaql.server.agents.coordination.zookeeper.EZKCoordinationExtension;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


/**
 * Created by dnlopes on 30/10/15.
 */
public final class Topology
{

	private static final Logger LOG = LoggerFactory.getLogger(Topology.class);
	public static String TOPOLOGY_FILE;
	public static String ZOOKEEPER_CONNECTION_STRING;

	private volatile static boolean IS_CONFIGURED = false;
	private static Topology instance;

	private Map<Integer, NodeConfig> replicators;
	private Map<Integer, NodeConfig> proxies;
	private Map<Integer, NodeConfig> coordinators;
	private Map<Integer, DatabaseProperties> databases;

	private Topology(String topologyFile) throws ConfigurationLoadException
	{
		if(topologyFile == null)
			throw new ConfigurationLoadException("topology file is null");

		TOPOLOGY_FILE = topologyFile;

		this.replicators = new HashMap<>();
		this.proxies = new HashMap<>();
		this.coordinators = new HashMap<>();
		this.databases = new HashMap<>();

		loadTopology();
		IS_CONFIGURED = true;
	}

	public static Topology getInstance()
	{
		return instance;
	}

	public static synchronized void setupTopology(String topologyFile) throws ConfigurationLoadException
	{
		if(IS_CONFIGURED)
			LOG.warn("topology configuration already loaded");
		else
			instance = new Topology(topologyFile);
	}

	private void loadTopology() throws ConfigurationLoadException
	{
		try
		{
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();

			InputStream stream = new FileInputStream(TOPOLOGY_FILE);
			Document doc = dBuilder.parse(stream);
			doc.getDocumentElement().normalize();

			NodeList rootList = doc.getElementsByTagName("config");
			Node config = rootList.item(0);
			NodeList nodeList = config.getChildNodes();

			for(int i = 0; i < nodeList.getLength(); i++)
			{
				Node n = nodeList.item(i);
				if(n.getNodeType() != Node.ELEMENT_NODE)
					continue;

				if(n.getNodeName().compareTo("topology") == 0)
					parseTopology(n);
			}

		} catch(ParserConfigurationException | IOException | SAXException e)
		{
			throw new ConfigurationLoadException(e.getMessage());
		}
	}

	private void parseTopology(Node node)
	{
		NodeList nodeList = node.getChildNodes();

		for(int i = 0; i < nodeList.getLength(); i++)
		{
			Node n = nodeList.item(i);
			if(n.getNodeType() != Node.ELEMENT_NODE)
				continue;

			if(n.getNodeName().compareTo("replicators") == 0)
				parseReplicators(n);
			if(n.getNodeName().compareTo("coordinators") == 0)
				parseCoordinators(n);
			if(n.getNodeName().compareTo("proxies") == 0)
				parseProxies(n);
			if(n.getNodeName().compareTo("databases") == 0)
				parseDatabases(n);
		}

		StringBuffer buffer = new StringBuffer();

		Iterator<NodeConfig> coordinatorConfigsIt = this.coordinators.values().iterator();
		while(coordinatorConfigsIt.hasNext())
		{
			NodeConfig zookConfig = coordinatorConfigsIt.next();
			buffer.append(zookConfig.getHost());
			buffer.append(":");
			buffer.append(zookConfig.getPort());

			if(coordinatorConfigsIt.hasNext())
				buffer.append(",");
		}

		ZOOKEEPER_CONNECTION_STRING = buffer.toString();
	}

	private void parseDatabases(Node node)
	{
		NodeList nodeList = node.getChildNodes();

		for(int i = 0; i < nodeList.getLength(); i++)
		{
			Node n = nodeList.item(i);
			if(n.getNodeType() != Node.ELEMENT_NODE)
				continue;

			if(n.getNodeName().compareTo("database") == 0)
				createDatabase(n);
		}
	}

	private void createDatabase(Node n)
	{
		NamedNodeMap map = n.getAttributes();
		String id = map.getNamedItem("id").getNodeValue();
		String dbHost = map.getNamedItem("dbHost").getNodeValue();
		String dbPort = map.getNamedItem("dbPort").getNodeValue();
		String dbUser = map.getNamedItem("dbUser").getNodeValue();
		String dbPwd = map.getNamedItem("dbPwd").getNodeValue();

		DatabaseProperties prop = new DatabaseProperties(dbUser, dbPwd, dbHost, Integer.parseInt(dbPort));
		databases.put(Integer.parseInt(id), prop);
	}

	private void parseReplicators(Node node)
	{
		NodeList nodeList = node.getChildNodes();

		for(int i = 0; i < nodeList.getLength(); i++)
		{
			Node n = nodeList.item(i);
			if(n.getNodeType() != Node.ELEMENT_NODE)
				continue;

			if(n.getNodeName().compareTo("replicator") == 0)
				createReplicator(n);
		}
	}

	private void parseProxies(Node node)
	{
		NodeList nodeList = node.getChildNodes();

		for(int i = 0; i < nodeList.getLength(); i++)
		{
			Node n = nodeList.item(i);
			if(n.getNodeType() != Node.ELEMENT_NODE)
				continue;

			if(n.getNodeName().compareTo("proxy") == 0)
				createProxy(n);
		}
	}

	private void parseCoordinators(Node node)
	{
		NodeList nodeList = node.getChildNodes();

		for(int i = 0; i < nodeList.getLength(); i++)
		{
			Node n = nodeList.item(i);
			if(n.getNodeType() != Node.ELEMENT_NODE)
				continue;

			if(n.getNodeName().compareTo("coordinator") == 0)
				createCoordinator(n);
		}
	}

	private void createCoordinator(Node n)
	{
		NamedNodeMap map = n.getAttributes();
		String id = map.getNamedItem("id").getNodeValue();
		String host = map.getNamedItem("host").getNodeValue();
		String port = null;

		if(map.getNamedItem("port") != null)
			port = map.getNamedItem("port").getNodeValue();

		NodeConfig newCoordinator;

		if(port != null)
			newCoordinator = new NodeConfig(Role.COORDINATOR, Integer.parseInt(id), host, Integer.parseInt(port),
					null);
		else
			newCoordinator = new NodeConfig(Role.COORDINATOR, Integer.parseInt(id), host,
					EZKCoordinationExtension.ZookeeperDefaults.ZOOKEEPER_DEFAULT_PORT, null);

		coordinators.put(Integer.parseInt(id), newCoordinator);
	}

	private void createReplicator(Node n)
	{
		NamedNodeMap map = n.getAttributes();
		String id = map.getNamedItem("id").getNodeValue();
		String host = map.getNamedItem("host").getNodeValue();
		String port = map.getNamedItem("port").getNodeValue();
		String refDatabase = map.getNamedItem("refDatabase").getNodeValue();

		DatabaseProperties props = this.databases.get(Integer.parseInt(refDatabase));

		NodeConfig newReplicator = new NodeConfig(Role.REPLICATOR, Integer.parseInt(id), host, Integer.parseInt(port),
				props);

		replicators.put(Integer.parseInt(id), newReplicator);
	}

	private void createProxy(Node n)
	{
		NamedNodeMap map = n.getAttributes();
		String id = map.getNamedItem("id").getNodeValue();
		String host = map.getNamedItem("host").getNodeValue();
		String port = map.getNamedItem("port").getNodeValue();
		String refDatabase = map.getNamedItem("refDatabase").getNodeValue();
		String refReplicator = map.getNamedItem("refReplicator").getNodeValue();

		NodeConfig replicatorConfig = this.getReplicatorConfigWithIndex(Integer.parseInt(refReplicator));
		DatabaseProperties props = this.databases.get(Integer.parseInt(refDatabase));

		NodeConfig newProxy = new NodeConfig(Role.PROXY, Integer.parseInt(id), host, Integer.parseInt(port), props,
				replicatorConfig);

		proxies.put(Integer.parseInt(id), newProxy);
	}

	public NodeConfig getProxyConfigWithIndex(int index)
	{
		return this.proxies.get(index);
	}

	public NodeConfig getReplicatorConfigWithIndex(int index)
	{
		return this.replicators.get(index);
	}

	public Map<Integer, NodeConfig> getAllReplicatorsConfig()
	{
		return replicators;
	}

	public int getReplicatorsCount()
	{
		return this.replicators.size();
	}

	public int getEmulatorsCount()
	{
		return this.proxies.size();
	}
}
