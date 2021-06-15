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

	// host thrift����˵�IP��ַ
	// port thrift����˵Ķ˿ں�
	// connect_timeout_milliseconds ����thrift����˵ĳ�ʱ������
	// receive_timeout_milliseconds ����thrift����˷����������ݵĳ�ʱ������
	// send_timeout_milliseconds ��thrift����˷�������ʱ�ĳ�ʱ������
	// set_log_function �Ƿ�����д��־������Ĭ������Ϊdebug������־
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

	// ֧��ָ�����servers������ʱ���ѡ��һ������һ���쳣ʱ�Զ�ѡ������
	// num_retries ���Դ���
	// retry_interval ���Լ������λΪ��
	// max_consecutive_failures ����Server�������ʧ�ܴ���
	// randomize_ �Ƿ����ѡ��һ��Server
	// always_try_last �Ƿ������������һ��Server
	// set_log_function �Ƿ�����д��־������Ĭ������Ϊdebug������־
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


	// ����thrift�����
	// ����ʱ�����׳����¼���thrift�쳣��
	// apache::thrift::transport::TTransportException
	// apache::thrift::TApplicationException
	// apache::thrift::TException
	bool connect()
	{
		try
		{
			if (!_transport->isOpen())
			{
				// ���TransportΪTFramedTransport����ʵ�ʵ��ã�TFramedTransport::open -> TSocketPool::open
				_transport->open();
				// ��"TSocketPool::open: all connections failed"ʱ��TSocketPool::open���׳��쳣TTransportException���쳣����ΪTTransportException::NOT_OPEN
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


	// �Ͽ���thrift����˵�����
	// ����ʱ�����׳����¼���thrift�쳣��
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

	// ���ؿɶ��ı�ʶ�������ڼ�¼��־
	uint16_t get_port() const 	// ȡthrift����˵Ķ˿ں�
	{
		return static_cast<uint16_t>(_socket->getPort());
	}

	std::string get_host() const	// ȡthrift����˵�IP��ַ
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

		// TransportĬ��Ϊapache::thrift::transport::TFramedTransport
		_transport.reset(new apache::thrift::transport::TBufferedTransport(_socket));
		// ProtocolĬ��Ϊapache::thrift::protocol::TBinaryProtocol
		_protocol.reset(new apache::thrift::protocol::TBinaryProtocol(_transport));
		// ����˵�Client
		_client.reset(new ThriftClient(_protocol));

		dynamic_cast<apache::thrift::protocol::TBinaryProtocol*>(_protocol.get())->setStringSizeLimit(DEFAULT_MAX_STRING_SIZE);
		dynamic_cast<apache::thrift::protocol::TBinaryProtocol*>(_protocol.get())->setStrict(false, false);
	}

public:
	int																	_connect_timeout_milliseconds;
	int																	_receive_timeout_milliseconds;
	int																	_send_timeout_milliseconds;


	// TSocketֻ֧��һ��server����TSocketPool��TSocket������֧��ָ�����server������ʱ���ѡ��һ��
	std::shared_ptr<ThriftClient>										_client;
	std::shared_ptr<apache::thrift::transport::TSocket>					_socket;
	std::shared_ptr<apache::thrift::transport::TTransport>				_transport;
	std::shared_ptr<apache::thrift::protocol::TProtocol>				_protocol;
};


