package com.paas.zk.zookeeper;

import com.paas.zk.constants.ConfigCenterConstants;
import com.paas.zk.constants.ZkCodeConstants;
import com.paas.zk.util.ZKUtil;
import com.paas.zk.zookeeper.impl.ZKPool;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;

import java.util.List;


public class ZKClient {
	private CuratorFramework client = null;
	private String zkAddr = null;// ip:port
	private int timeOut = 20000;
	private String authSchema = null;
	private String authInfo = null;
	private ZKPool pool = null;

	public ZKClient(String zkAddr, int timeOut, String... auth)
			throws Exception {
//		Assert.assertNotNull(zkAddr, "zkAddress should not be Null");
		if(zkAddr==null||"".equals(zkAddr))
			throw new Exception("zkAddress should not be Null");
		
		this.zkAddr = zkAddr.trim();
		if (timeOut > 0) {
			this.timeOut = timeOut;
		}
		if (null != auth && auth.length >= 2) {
			if (auth[0]!=null&&!"".equals(auth[0].trim())) {
				authSchema = auth[0].trim();
			}
			if (auth[1]!=null&&!"".equals(auth[1].trim())) {
				authInfo = auth[1].trim();
			}
		}
		CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory
				.builder().connectString(this.zkAddr)
				.connectionTimeoutMs(this.timeOut)
				.retryPolicy(new RetryNTimes(Integer.MAX_VALUE, 10));
		if (authSchema!=null&&!"".equals(authSchema.trim()) && 
				authInfo!=null&&!"".equals(authInfo.trim()) ) {
			builder.authorization(authSchema, authInfo.getBytes());
		}

		client = builder.build();
		client.start();
		client.blockUntilConnected();
	}

	public String getNodeData(String nodePath, boolean watch) throws Exception {
		byte[] data;
		if (watch) {
			data = client.getData().watched().forPath(nodePath);
		} else {
			data = client.getData().forPath(nodePath);
		}
		if (null == data || data.length <= 0)
			return null;
		return new String(data, ZkCodeConstants.CHARSET_UTF8);
	}

	public String getNodeData(String nodePath) throws Exception {
		return getNodeData(nodePath, false);
	}

	public String getNodeData(String nodePath, Watcher watcher)
			throws Exception {
		byte[] data = getNodeBytes(nodePath, watcher);
		return new String(data, ZkCodeConstants.CHARSET_UTF8);
	}

	public byte[] getNodeBytes(String nodePath, Watcher watcher)
			throws Exception {
		byte[] bytes = null;
		if (null != watcher)
			bytes = client.getData().usingWatcher(watcher).forPath(nodePath);
		else
			bytes = client.getData().forPath(nodePath);

		return bytes;
	}

	public byte[] getNodeBytes(String nodePath) throws Exception {
		return getNodeBytes(nodePath, null);
	}

	public void createNode(String nodePath, String data, CreateMode createMode)
			throws Exception {
		createNode(nodePath, Ids.OPEN_ACL_UNSAFE, data, createMode);
	}

	public void createNode(String nodePath, List<ACL> acls, String data,
			CreateMode createMode) throws Exception {
		byte[] bytes = null;
		if (data!=null&&!"".equals(data.trim()))
			bytes = data.getBytes(ZkCodeConstants.CHARSET_UTF8);
		createNode(nodePath, acls, bytes, createMode);
	}

	public void createNode(String nodePath, List<ACL> acls, byte[] data,
			CreateMode createMode) throws Exception {
		//判断是否路径带着/如果没带，加上
		nodePath=ZKUtil.processPath(nodePath);
		client.create().creatingParentsIfNeeded().withMode(createMode)
				.withACL(acls).forPath(nodePath, data);
	}

	public void createNode(String nodePath, String data) throws Exception {
		createNode(nodePath, data, CreateMode.PERSISTENT);
	}

	public void setNodeData(String nodePath, String data) throws Exception {
		byte[] bytes = null;
		if (data!=null&&!"".equals(data.trim()))
			bytes = data.getBytes(ZkCodeConstants.CHARSET_UTF8);
		setNodeData(nodePath, bytes);
	}

	public void setNodeData(String nodePath, byte[] data) throws Exception {
		client.setData().forPath(nodePath, data);
	}

	public List<String> getChildren(String nodePath, Watcher watcher)
			throws Exception {
		return client.getChildren().usingWatcher(watcher).forPath(nodePath);
	}

	/**
	 * 在目录下创建顺序节点
	 *
	 * @param nodePath
	 * @throws Exception
	 */
	public void createSeqNode(String nodePath) throws Exception {
		nodePath=ZKUtil.processPath(nodePath);
		client.create().creatingParentsIfNeeded()
				.withMode(CreateMode.PERSISTENT_SEQUENTIAL)
				.withACL(Ids.OPEN_ACL_UNSAFE).forPath(nodePath);
	}

	public boolean exists(String path) throws Exception {
		return null == client.checkExists().forPath(path) ? false : true;
	}

	public boolean exists(String path, Watcher watcher) throws Exception {
		if (null != watcher) {
			return null == client.checkExists().watched().forPath(path) ? false
					: true;
		} else {
			return null == client.checkExists().forPath(path) ? false : true;
		}
	}

	public boolean isConnected() {
		if (null == client
				|| !CuratorFrameworkState.STARTED.equals(client.getState())) {
			return false;
		}
		return true;
	}

	public void retryConnection() {
		client.start();
	}

	public List<String> getChildren(String path) throws Exception {
		return client.getChildren().forPath(path);
	}

	public List<String> getChildren(String path, boolean watcher)
			throws Exception {
		if (watcher) {
			return client.getChildren().watched().forPath(path);
		} else {
			return client.getChildren().forPath(path);
		}
	}

	public void deleteNode(String path) throws Exception {
		client.delete().guaranteed().deletingChildrenIfNeeded().forPath(path);
	}

	public void quit() {
		if (null != client
				&& CuratorFrameworkState.STARTED.equals(client.getState())) {
			client.close();
		}
	}

	public ZKPool getPool() {
		return pool;
	}

	public void setPool(ZKPool pool) {
		this.pool = pool;
	}

	public ZKClient addAuth(final String authSchema, final String authInfo)
			throws Exception {
		this.client
				.getZookeeperClient()
				.getZooKeeper()
				.addAuthInfo(ConfigCenterConstants.ZKAuthSchema.DIGEST,
						authInfo.getBytes());
		return this;
	}

	public InterProcessLock getInterProcessLock(String lockPath) {
		return new InterProcessMutex(client, lockPath);
	}
}
