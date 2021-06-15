#pragma once
#include <string.h>
#include "hbase/THBaseService.h"
#include "boost/lockfree/queue.hpp"
#include "thriftclient.h"
#include "singleton.h"
#include "container.h"
#include "timer.h"

using namespace apache::hadoop::hbase::thrift2;

namespace hbase {
	namespace thrift2 {

		class CGet;
		class CScan;
		class CMulitGet;
		class CHBaseQuery;
		class CHBaseThrift;
		/////////////////////////////////////////// STRUCT && CLASS /////////////////////////////////////////////
		struct CHBasePrivate 
		{
			int			send_timeout = 2000;
			int			connect_timeout = 2000;			// 连接超时
			int			recive_timeout = 2000;				// 接收超时
			std::string host_list = "";					// 连接源
		};

		class CHBaseConnPool	// 连接池
		{
		public:
			explicit CHBaseConnPool(const CHBasePrivate &pri);
			~CHBaseConnPool() {}

			bool  InitConnpool(int maxSize);
			std::shared_ptr<CThriftClientHelper<THBaseServiceClient>> GetConnection();
			void  DestoryConnPool();			// 销毁连接池	
			void  ReleaseConnection(std::shared_ptr<CThriftClientHelper<THBaseServiceClient>> conn, bool bRelease = true);
			void  onTimer();
		protected:
			std::shared_ptr<CThriftClientHelper<THBaseServiceClient>> getFreeConn();
			std::shared_ptr<CThriftClientHelper<THBaseServiceClient>> createConnection();			// 创建一个新连接
			void  putFreeConn(std::shared_ptr<CThriftClientHelper<THBaseServiceClient>> conn);

		private:

			int																  m_maxSize;		// 连接池的最大连接数
			std::atomic<int>												  m_curSize;		// 当前连接池里活跃的连接数
			CHBasePrivate													  m_private;		// 私有数据
			std::vector<std::pair<std::string, int>>						  m_servers;
			threadsafe_list<CThriftClientHelper<THBaseServiceClient>>		  m_connList;
			CTimer<boost::posix_time::milliseconds>							  m_timer;
		};

		class CPut
		{
		public:
			CPut();
			~CPut();
			void setRowkey(const std::string& rowkey);
			void appendColumn(const std::string &family, const std::string &qualifier, const std::string &value);
			void setDurability(TDurability::type durability = TDurability::SYNC_WAL);
			friend CHBaseQuery;
		private:
			apache::hadoop::hbase::thrift2::TPut					  m_put;
			std::vector<apache::hadoop::hbase::thrift2::TColumnValue> m_familys;
		};

		class CGet
		{
		public:
			CGet();
			~CGet();
			void setMaxVersion(const uint16_t &version = 0);
			void setRowkey(const std::string& rowkey);
			void setFilterString(const char* format, ...);
			void appendColumn(const std::string &family, const std::string &qualifier);
			void setTimeRange(const int64_t &begin, const int64_t &end);
			friend CMulitGet;
			friend CHBaseQuery;
		private:
			apache::hadoop::hbase::thrift2::TGet					m_get;
			std::vector<apache::hadoop::hbase::thrift2::TColumn>	m_familys;
		};

		class CMulitGet
		{
		public:
			CMulitGet() {}
			~CMulitGet() {}
			void clear() { m_gets.clear(); }
			void appendGet(CGet &get) { 
				get.m_get.__set_columns(get.m_familys);
				m_gets.push_back(get.m_get);
			}
			friend CHBaseQuery;
		private:
			std::vector<apache::hadoop::hbase::thrift2::TGet>   m_gets;
		};

		class CScan
		{
		public:	
			CScan();
			~CScan();
			void setCaching(const int &count);				// 需要查询的行数
			void setBatchSize(const int &size);				// 需要查询的列数
			void setReversed(const bool &rev = false);
			void setMaxVersion(const uint16_t &version = 0);
			void setFilterString(const char* format, ...);
			void setTimeRange(const int64_t &begin, const int64_t &end);	
			void setRowRange(const std::string& begin_row, const std::string& stop_row);
			void appendColumn(const std::string &family, const std::string &qualifier);
			friend CHBaseQuery;
		private:
			int														m_nCacheRows;
			apache::hadoop::hbase::thrift2::TScan					m_scan;
			std::vector<apache::hadoop::hbase::thrift2::TColumn>	m_familys;
		};

		// 线程不安全，禁止多个线程共用一个query
		class CHBaseQuery
		{
		public:
			CHBaseQuery(std::shared_ptr<CThriftClientHelper<THBaseServiceClient>> client);
			~CHBaseQuery();

			bool nextColumn();
			bool nextRow();
			bool execGet(const std::string &table, CGet &get);
			bool execPut(const std::string &table, CPut &put);
			bool execMulitGet(const std::string &table, CMulitGet &mulit_get);
			bool execScan(const std::string &table, CScan &scan);
			void setRetryTimes(const int &count);									// 设置重试次数
			std::string getRowkey();
			std::string getFamilyName();
			std::string getColumnName();
			std::string getColumnValue();
			uint64_t getTimestamp();
			std::shared_ptr<CThriftClientHelper<THBaseServiceClient>> getConnection();
		private:
			int																		  m_retryTimes;
			std::shared_ptr<CThriftClientHelper<THBaseServiceClient>>				  m_client;
			std::vector<apache::hadoop::hbase::thrift2::TResult>					  m_result;
			std::vector<apache::hadoop::hbase::thrift2::TResult>::const_iterator	  m_RowIter;			// row iter
			std::vector<apache::hadoop::hbase::thrift2::TResult>::const_iterator	  m_RowCurrIter;		// row curr iter
			std::vector<apache::hadoop::hbase::thrift2::TColumnValue>::const_iterator m_CloumnIter;			// cloumn iter
			std::vector<apache::hadoop::hbase::thrift2::TColumnValue>::const_iterator m_CloumnCurrIter;		// cloumn curr iter
		};

		class CHBaseThrift
		{
			SINGLETON(CHBaseThrift);
			bool close();
			bool open(int size = 1);
			void setHostlist(const std::string &lists);
			void setTimeout(const int &c_timeout = 2000, const int &r_timeout = 2000, const int &s_timeout = 2000);
			void releaseQuery(CHBaseQuery * pQuery, bool bRelease = true);
			CHBaseQuery * getQuery();
		private:
			CHBasePrivate						m_private;
			std::unique_ptr<CHBaseConnPool>		m_pConnPool;
		};
	}
} // namespace end of hbase