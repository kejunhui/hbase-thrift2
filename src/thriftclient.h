#pragma once
#include <memory>
#include <string>
#include <thrift/thrift-config.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocketPool.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TTransportException.h>
#include "commonest.h"
#include "log.h"

//apache::thrift::protocol::TBinaryProtocol,apache::thrift::transport::TFramedTransport
template <class ThriftClient>
class CThriftClientHelper
{
public:
	static const int DEFAULT_MAX_STRING_SIZE = 256 * 1024 * 1024;

	// host thrift服务端的IP地址
	// port thrift服务端的端口号
	// connect_timeout_milliseconds 连接thrift服务端的超时毫秒数
	// receive_timeout_milliseconds 接收thrift服务端发过来的数据的超时毫秒数
	// send_timeout_milliseconds 向thrift服务端发送数据时的超时毫秒数
	// set_log_function 是否设置写日志函数，默认设置为debug级别日志
	CThriftClientHelper(const std::string &host, uint16_t port,int connect_timeout_milliseconds = 2000,
		int receive_timeout_milliseconds = 2000,int send_timeout_milliseconds = 2000)
		: _connect_timeout_milliseconds(connect_timeout_milliseconds),
		_receive_timeout_milliseconds(receive_timeout_milliseconds),
		_send_timeout_milliseconds(send_timeout_milliseconds)
	{
		apache::thrift::GlobalOutput.setOutputFunction(ThriftLog);
		_socket.reset(new apache::thrift::transport::TSocket(host, port));
		init();
	}

	// 支持指定多个servers，运行时随机选择一个，当一个异常时自动选择其它
	// num_retries 重试次数
	// retry_interval 重试间隔，单位为秒
	// max_consecutive_failures 单个Server最大连续失败次数
	// randomize_ 是否随机选择一个Server
	// always_try_last 是否总是重试最后一个Server
	// set_log_function 是否设置写日志函数，默认设置为debug级别日志
	CThriftClientHelper(const std::vector<std::pair<std::string, int> >& servers,
		int connect_timeout_milliseconds = 2000,
		int receive_timeout_milliseconds = 2000,
		int send_timeout_milliseconds = 2000,
		int num_retries = 1, int retry_interval = 60,
		int max_consecutive_failures = 1,
		bool randomize = true, bool always_try_last = true)
		: _connect_timeout_milliseconds(connect_timeout_milliseconds),
		_receive_timeout_milliseconds(receive_timeout_milliseconds),
		_send_timeout_milliseconds(send_timeout_milliseconds)
	{
		apache::thrift::GlobalOutput.setOutputFunction(ThriftLog);
		apache::thrift::transport::TSocketPool* socket_pool = new apache::thrift::transport::TSocketPool(servers);
		socket_pool->setNumRetries(num_retries);
		socket_pool->setRetryInterval(retry_interval);
		socket_pool->setMaxConsecutiveFailures(max_consecutive_failures);
		socket_pool->setRandomize(randomize);
		socket_pool->setAlwaysTryLast(always_try_last);
		_socket.reset(socket_pool);
		init();
	}

	~CThriftClientHelper()
	{
		close();
	}


	// 连接thrift服务端
	// 出错时，可抛出以下几个thrift异常：
	// apache::thrift::transport::TTransportException
	// apache::thrift::TApplicationException
	// apache::thrift::TException
	bool connect()
	{
		try
		{
			if (!_transport->isOpen())
			{
				// 如果Transport为TFramedTransport，则实际调用：TFramedTransport::open -> TSocketPool::open
				_transport->open();
				// 当"TSocketPool::open: all connections failed"时，TSocketPool::open就抛出异常TTransportException，异常类型为TTransportException::NOT_OPEN
			}
		}
		catch (apache::thrift::transport::TTransportException& ex)
		{
			LERROR("connect {}, transport(I/O) exception: ({})({})", str().c_str(), ex.getType(), ex.what());
			return false;
		}
		catch (apache::thrift::TApplicationException& ex)
		{
			LERROR("connect {} application exception:{},{}", str().c_str(), ex.getType(), ex.what());
			return false;
		}
		return true;
	}

	bool reconnect()
	{
		close();
		return connect();
	}

	bool is_connected() const
	{
		return _transport && _transport->isOpen();
	}


	// 断开与thrift服务端的连接
	// 出错时，可抛出以下几个thrift异常：
	// apache::thrift::transport::TTransportException
	// apache::thrift::TApplicationException
	// apache::thrift::TException
	void close()
	{
		try
		{
			if (_transport && _transport->isOpen())
			{
				_transport->close();
			}
		}
		catch (apache::thrift::transport::TTransportException& ex)
		{
			LERROR("close {}, transport(I/O) exception: ({})({})", str().c_str(), ex.getType(), ex.what());
		}
		catch (apache::thrift::TApplicationException& ex)
		{
			LERROR("close {} application exception:{},{}", str().c_str(), ex.getType(), ex.what());
		}
	}

	apache::thrift::transport::TSocket* get_socket() { return _socket.get(); }
	const apache::thrift::transport::TSocket get_socket() const { return _socket.get(); }
	ThriftClient* get() { return _client.get(); }
	ThriftClient* get() const { return _client.get(); }
	ThriftClient* operator ->() { return get(); }
	ThriftClient* operator ->() const { return get(); }

	// 返回可读的标识，常用于记录日志
	uint16_t get_port() const 	// 取thrift服务端的端口号
	{
		return static_cast<uint16_t>(_socket->getPort());
	}

	std::string get_host() const	// 取thrift服务端的IP地址
	{
		return _socket->getHost();
	}

	std::string str() const
	{
		return FormatString("thrift://%s:%u", get_host().c_str(), get_port());
	}

private:
	void init()
	{
		_socket->setConnTimeout(_connect_timeout_milliseconds);
		_socket->setRecvTimeout(_receive_timeout_milliseconds);
		_socket->setSendTimeout(_send_timeout_milliseconds);

		// Transport默认为apache::thrift::transport::TFramedTransport
		_transport.reset(new apache::thrift::transport::TBufferedTransport(_socket));
		// Protocol默认为apache::thrift::protocol::TBinaryProtocol
		_protocol.reset(new apache::thrift::protocol::TBinaryProtocol(_transport));
		// 服务端的Client
		_client.reset(new ThriftClient(_protocol));

		dynamic_cast<apache::thrift::protocol::TBinaryProtocol*>(_protocol.get())->setStringSizeLimit(DEFAULT_MAX_STRING_SIZE);
		dynamic_cast<apache::thrift::protocol::TBinaryProtocol*>(_protocol.get())->setStrict(false, false);
	}

public:
	int																	_connect_timeout_milliseconds;
	int																	_receive_timeout_milliseconds;
	int																	_send_timeout_milliseconds;


	// TSocket只支持一个server，而TSocketPool是TSocket的子类支持指定多个server，运行时随机选择一个
	std::shared_ptr<ThriftClient>										_client;
	std::shared_ptr<apache::thrift::transport::TSocket>					_socket;
	std::shared_ptr<apache::thrift::transport::TTransport>				_transport;
	std::shared_ptr<apache::thrift::protocol::TProtocol>				_protocol;
};


