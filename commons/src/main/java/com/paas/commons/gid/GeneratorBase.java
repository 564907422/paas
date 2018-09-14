package com.paas.commons.gid;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

/**
 *
 *         序列号生成器的抽象基类
 */
abstract class GeneratorBase {

	/***** 数据中心ID和节点ID共占10位，共支持部署1024个节点 *****/
	// 数据中心ID位数
	final long dcIdBits = 1L;
	// 工作节点ID位数
	final long workerIdBits = 10L - dcIdBits;

	long initWorkerId() {
		//获得机器的ip
		String ip = getLocalIp();
        ip=ip.split("\\.")[3];
		return new Long(ip);
	}

	String getLocalIp() {
		String addr = "";
		try{
//			addr = InetAddress.getLocalHost().getHostAddress();//获得本机IP
			Enumeration<NetworkInterface> allNetInterfaces = NetworkInterface.getNetworkInterfaces();
			while (allNetInterfaces.hasMoreElements()){
				NetworkInterface netInterface = (NetworkInterface) allNetInterfaces.nextElement();
				Enumeration<InetAddress> addresses = netInterface.getInetAddresses();
				while (addresses.hasMoreElements()){
					InetAddress ip = (InetAddress) addresses.nextElement();
					if (ip != null
							&& ip instanceof Inet4Address
							&& !ip.isLoopbackAddress() //loopback地址即本机地址，IPv4的loopback范围是127.0.0.0 ~ 127.255.255.255
							&& ip.getHostAddress().indexOf(":")==-1){
//						System.out.println("本机的IP = " + ip.getHostAddress());
						return ip.getHostAddress();
					}
				}
			}
			if("127.0.0.1".equals(addr)){
				addr = RandomSn.generateRandomNumber(6)+"";
			}
		}catch (Exception e){
			e.printStackTrace();
			addr = RandomSn.generateRandomNumber(6)+"";
		}
		return addr;
	}

}
