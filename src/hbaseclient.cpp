#include <stdlib.h>
#include "hbaseclient.h"
#include "log.h"

#define CATCH(msg) \
catch (apache::hadoop::hbase::thrift2::TIOError& ex)\
{\
	LERROR("{} IOError: {}", msg, ex.message.c_str());\
}\
catch (apache::thrift::transport::TTransportException& ex)\
{\
	LERROR("{} {} transport exception: ({}){}", msg, table.c_str(), ex.getType(), ex.what());\
	m_client->reconnect();\
}\
catch (apache::thrift::TApplicationException& ex)\
{\
	LERROR("{} {} application exception: ({}){}", msg, table.c_str(), ex.getType(), ex.what());\
}\
catch (apache::hadoop::hbase::thrift2::TIllegalArgument& ex)\
{\
	LERROR("{} {} exception: {}", msg, table.c_str(), ex.message.c_str());\
}\
catch (apache::thrift::protocol::TProtocolException& ex)\
{\
	LERROR("{} {} protocol exception: ({}){}", msg, table.c_str(), ex.getType(), ex.what());\
}\

namespace hbase {
	namespace thrift2 {

		CHBaseConnPool::CHBaseConnPool(const CHBasePrivate &pri):m_private(pri),m_curSize(0), m_maxSize(0)
		{

		}

		bool CHBaseConnPool::InitConnpool(int maxSize)
		{
			m_maxSize = maxSize;
			LDEBUG("InitHBaseConnpool {}[{}]", m_private.host_list, maxSize);
			std::vector<std::string> host_array;
			StringSplit(host_array, m_private.host_list, ",");

			for (std::vector<std::string>::const_iterator iter = host_array.begin(); iter != host_array.end(); iter++)
			{
				std::vector<std::string> ip_port;
				StringSplit(ip_port, *iter, ":");
				if (ip_port.size() != 2)
				{
					continue;
				}
				const std::string& host_ip = ip_port[0];
				const std::string& host_port = ip_port[1];
				m_servers.push_back(std::make_pair(host_ip, atoi(host_port.c_str())));
			}

			for (int i = 0; i < m_maxSize / 2; ++i) // 初始化一半
			{
				std::shared_ptr<CThriftClientHelper<THBaseServiceClient>> conn = createConnection();
				if(conn) putFreeConn(conn);
			}
			m_timer.bind(std::bind(&CHBaseConnPool::onTimer, this));
			m_timer.start(30 * 1000); // 30sec
			return true;
		}

		std::shared_ptr<CThriftClientHelper<THBaseServiceClient>> CHBaseConnPool::GetConnection()
		{
			std::shared_ptr<CThriftClientHelper<THBaseServiceClient>> hbaseConn = getFreeConn();
			if (hbaseConn == NULL && m_curSize < m_maxSize)
			{
				hbaseConn = createConnection();
			}
			if (hbaseConn && !hbaseConn->is_connected())
			{
				m_curSize--;
				hbaseConn = createConnection();
			}
			return hbaseConn;
		}

		void CHBaseConnPool::DestoryConnPool()
		{
			std::shared_ptr<CThriftClientHelper<THBaseServiceClient>> hbaseConn;
			while ((hbaseConn = getFreeConn()) != nullptr)
			{
				hbaseConn->close();
			}
			m_curSize = 0;
		}

		void CHBaseConnPool::ReleaseConnection(std::shared_ptr<CThriftClientHelper<THBaseServiceClient>> conn, bool bRelease)
		{
			if (bRelease)
			{
				putFreeConn(conn);
			}
			else if (conn)
			{
				conn->close();
				m_curSize--;
			}
		}

		std::shared_ptr<CThriftClientHelper<THBaseServiceClient>> CHBaseConnPool::getFreeConn()
		{
			return m_connList.pop_front();
		}

		std::shared_ptr<CThriftClientHelper<THBaseServiceClient>> CHBaseConnPool::createConnection()
		{	
			std::shared_ptr<CThriftClientHelper<THBaseServiceClient>> conn = std::make_shared<CThriftClientHelper<THBaseServiceClient>>
				(m_servers, m_private.connect_timeout, m_private.recive_timeout, m_private.send_timeout);
			if (!conn->connect())
			{
				return nullptr;
			}
			m_curSize++;
			return conn;
		}

		void CHBaseConnPool::putFreeConn(std::shared_ptr<CThriftClientHelper<THBaseServiceClient>> conn)
		{
			m_connList.push_front(conn);
		}

		void CHBaseConnPool::onTimer()
		{
			// hbase thrift2 server 每隔一分钟会断开空闲连接，此时只能设置conf/hbase-site.xml 中的超时时间(治标不治本)，
			// hbase 出于某种目的，断开连接，或者想办法使连接保持存活
			//LDEBUG("HBaseConnpool ping[{}]:{}", m_curSize.load(), m_private.host_list.c_str());
		/*	apache::hadoop::hbase::thrift2::TIncrement increment;
			std::vector<apache::hadoop::hbase::thrift2::TColumnIncrement> family_increment_columns;
			increment.__set_durability(TDurability::SYNC_WAL);
			std::string row_key = CDateTime::currentDateTime().ToString3();
			increment.__set_row(row_key);
			apache::hadoop::hbase::thrift2::TColumnIncrement  family_column;
			family_column.__set_family("ping");
			family_column.__set_qualifier("count");
			family_column.__set_amount(1);
			family_increment_columns.push_back(family_column);
			increment.__set_columns(family_increment_columns);
			m_connList.for_each([&increment](CThriftClientHelper<THBaseServiceClient> *client) {
				apache::hadoop::hbase::thrift2::TResult result;
				(*client)->increment(result, "ping_increment", increment);
			});*/
		}

		///////////////////////////////////////////////////////// CGet ///////////////////////////////////////////////////////////
		CGet::CGet()
		{

		}

		CGet::~CGet()
		{

		}

		void CGet::setRowkey(const std::string& rowkey)
		{
			m_get.__set_row(rowkey);
		}

		void CGet::setMaxVersion(const uint16_t &version)
		{
			m_get.__set_maxVersions(version);
		}

		void CGet::setFilterString(const char* format, ...)
		{
			va_list ap;
			va_start(ap, format);
			char buf[1024] = { 0 };
			vsprintf(buf, format, ap);
			va_end(ap);        //  清空参数指针
			m_get.__set_filterString(buf);
		}

		void CGet::appendColumn(const std::string &family, const std::string &qualifier)
		{
			apache::hadoop::hbase::thrift2::TColumn  family_column;
			family_column.__set_family(family);
			if (!qualifier.empty()) family_column.__set_qualifier(qualifier);
			m_familys.push_back(family_column);
		}

		void CGet::setTimeRange(const int64_t &begin, const int64_t &end)
		{
			TTimeRange tr;
			tr.__set_minStamp(begin);
			tr.__set_minStamp(end);
			m_get.__set_timeRange(tr);
		}
		////////////////////////////////////////////////// CPut ///////////////////////////////////////////////////////
		CPut::CPut()
		{
			m_put.__set_durability(TDurability::SYNC_WAL);
		}

		CPut::~CPut()
		{

		}

		void CPut::setRowkey(const std::string& rowkey)
		{
			m_put.__set_row(rowkey);
		}

		void CPut::appendColumn(const std::string &family, const std::string &qualifier, const std::string &value)
		{
			apache::hadoop::hbase::thrift2::TColumnValue family_column;
			family_column.__set_family(family);
			family_column.__set_qualifier(qualifier);
			family_column.__set_value(value);
			m_familys.push_back(family_column);
		}	

		void CPut::setDurability(TDurability::type durability)
		{
			m_put.__set_durability(durability);
		}

		////////////////////////////////////////////////// CScan ////////////////////////////////////////////////////////
		CScan::CScan():m_nCacheRows(0)
		{

		}

		CScan::~CScan()
		{

		}

		void CScan::setCaching(const int &count)	// 需要查询的行数
		{
			m_nCacheRows = count;
		}

		void CScan::setBatchSize(const int &size)	// 每次获取的列数
		{
			m_scan.__set_batchSize(size);
		}

		void CScan::setReversed(const bool &rev)
		{
			m_scan.__set_reversed(rev);
		}

		void CScan::setMaxVersion(const uint16_t &version)
		{
			m_scan.__set_maxVersions(version);
		}

		void CScan::setFilterString(const char* format, ...)
		{
			va_list ap;
			va_start(ap, format);
			char buf[1024] = { 0 };
			vsprintf(buf, format, ap);
			va_end(ap);        //  清空参数指针
			m_scan.__set_filterString(buf);
		}

		void CScan::setTimeRange(const int64_t &begin, const int64_t &end)
		{
			TTimeRange tr;
			tr.__set_minStamp(begin);
			tr.__set_minStamp(end);
			m_scan.__set_timeRange(tr);
		}

		void CScan::setRowRange(const std::string& begin_row, const std::string& stop_row)
		{
			m_scan.__set_startRow(begin_row);
			m_scan.__set_stopRow(stop_row);
		}

		void CScan::appendColumn(const std::string &family, const std::string &qualifier)
		{
			apache::hadoop::hbase::thrift2::TColumn  family_column;
			family_column.__set_family(family);
			if(!qualifier.empty()) family_column.__set_qualifier(qualifier);
			m_familys.push_back(family_column);
		}

		//////////////////////////////////////////////// CHBaseQuery ///////////////////////////////////////////////////
		CHBaseQuery::CHBaseQuery(std::shared_ptr<CThriftClientHelper<THBaseServiceClient>> client):m_client(client), m_retryTimes(2)
		{

		}

		CHBaseQuery::~CHBaseQuery()
		{
			m_result.clear();
		}

		bool CHBaseQuery::nextRow()
		{
			if (m_result.end() == m_RowIter) return false;
			m_CloumnIter = m_RowIter->columnValues.begin();
			m_RowCurrIter = m_RowIter;
			m_RowIter++;
			return true;
		}

		bool CHBaseQuery::nextColumn()
		{
			if (m_RowCurrIter->columnValues.end() == m_CloumnIter) return false;
			m_CloumnCurrIter = m_CloumnIter;
			m_CloumnIter++;
			return true;
		}

		bool CHBaseQuery::execGet(const std::string &table, CGet &get)
		{
			m_result.clear();
			get.m_get.__set_columns(get.m_familys);
			for (int i = 0; i < m_retryTimes; ++i){
				try{
					apache::hadoop::hbase::thrift2::TResult ret;
					(*m_client)->get(ret, table, get.m_get);
					m_result.push_back(ret);
					m_RowIter = m_result.begin();
					return true;
				}
				CATCH("exec get from")
			}
			return false;
		}

		bool  CHBaseQuery::execPut(const std::string &table, CPut &put)
		{
			put.m_put.__set_columnValues(put.m_familys);
			for (int i = 0; i < m_retryTimes; ++i) {
				try {
					(*m_client)->put(table, put.m_put);
					return true;
				}
				CATCH("exec put to")
			}
			return false;
		}

		bool CHBaseQuery::execMulitGet(const std::string &table, CMulitGet &mulit_get)
		{
			m_result.clear();
			for (int i = 0; i < m_retryTimes; ++i) {
				try {
					(*m_client)->getMultiple(m_result, table, mulit_get.m_gets);
					m_RowIter = m_result.begin();
					return true;
				}
				CATCH("exec mulit get from")
			}
			return false;
		}

		bool CHBaseQuery::execScan(const std::string &table, CScan &scan)
		{
			m_result.clear();
			int32_t caching = scan.m_nCacheRows * scan.m_familys.size();
			scan.m_scan.__set_caching(caching);
			scan.m_scan.__set_columns(scan.m_familys);
			for (int i = 0; i < m_retryTimes; ++i) {
				try {
					(*m_client)->getScannerResults(m_result, table, scan.m_scan, scan.m_nCacheRows);
					m_RowIter = m_result.begin();
					return true;
				}
				CATCH("exec scan from")
			}
			return false;
		}

		void CHBaseQuery::setRetryTimes(const int &count) // 设置重试次数
		{
			m_retryTimes = count;
		}	

		std::shared_ptr<CThriftClientHelper<THBaseServiceClient>> CHBaseQuery::getConnection()
		{
			return m_client;
		}

		std::string CHBaseQuery::getRowkey()
		{
			return m_RowCurrIter->row;
		}

		std::string CHBaseQuery::getFamilyName() 
		{
			return m_CloumnCurrIter->family;
		}

		std::string CHBaseQuery::getColumnName()
		{
			return m_CloumnCurrIter->qualifier;
		}

		std::string CHBaseQuery::getColumnValue()
		{
			return m_CloumnCurrIter->value;
		}

		uint64_t CHBaseQuery::getTimestamp()
		{
			return m_CloumnCurrIter->timestamp;
		}

		//////////////////////////////////////////////////////////////////////////////////////////////////

		CHBaseThrift::CHBaseThrift()
		{

		}

		CHBaseThrift::~CHBaseThrift()
		{

		}

		bool CHBaseThrift::open(int size)
		{
			m_pConnPool = std::make_unique<CHBaseConnPool>(m_private);
			m_pConnPool->InitConnpool(size);
			return true;
		}

		bool CHBaseThrift::close()
		{
			if (m_pConnPool) m_pConnPool->DestoryConnPool();
			return true;
		}	

		void CHBaseThrift::setHostlist(const std::string &lists)
		{
			m_private.host_list = lists;
		}

		void CHBaseThrift::setTimeout(const int &c_timeout, const int &r_timeout, const int &s_timeout)
		{
			m_private.connect_timeout = c_timeout;
			m_private.recive_timeout = r_timeout;
			m_private.send_timeout = s_timeout;
		}

		void CHBaseThrift::releaseQuery(CHBaseQuery * pQuery, bool bRelease)
		{
			std::shared_ptr<CThriftClientHelper<THBaseServiceClient>> conn = pQuery->getConnection();
			m_pConnPool->ReleaseConnection(conn, bRelease);
			delete pQuery;
		}

		CHBaseQuery * CHBaseThrift::getQuery()
		{
			CHBaseQuery * query = nullptr;
			std::shared_ptr<CThriftClientHelper<THBaseServiceClient>> pConn = m_pConnPool->GetConnection();
			if (pConn){
				query = new CHBaseQuery(pConn);
			}
			else LWARN("get query failed!");
			return query;
		}
	}
}