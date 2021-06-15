#pragma once
#include "count.h"

// 统计的最小粒度，小时
template<class T>
class MulitHourlyCount
{
public:
	int getStatisticsResult(const std::string &key, const CDateTime &minTime, const CDateTime &maxTime, T &result)
	{
		int64_t minsecs = minTime.GetGMTSeconds(); // 时间戳
		int64_t maxsecs = maxTime.GetGMTSeconds();
		if (maxsecs - minsecs > 3500) // 允许100s误差
		{
			std::unique_lock<std::mutex> lk(m_mutex);
			if (!m_hour_count.isEmpty()) {
				result = m_hour_count; // 读取缓存
			}
			else {
				CTimeRange range = std::make_pair(minsecs, maxsecs);
				if (m_hour_count.getResultFromHbase(key, range)) { // check hbase whether has an hour result
					result = m_hour_count;
				}
				else {
					m_hour_count.getHbaseCount(key, range);
					result = m_hour_count;
					m_hour_count.putResultToHbase(key, range);	// put hour count result to hbase;
				}
			}
		}
		else
		{
			T ret;
			std::unique_lock<std::mutex> lk(m_mutex);
			ret.getHbaseCount(key, std::make_pair(minsecs, maxsecs));
			result = ret;
		}
		return 0;
	}
private:
	std::mutex										m_mutex;
	T												m_hour_count;		// 
};

template<class T>
class MulitDailyCount
{
public:
	int getStatisticsResult(const std::string &key, const CDateTime &minTime, const CDateTime &maxTime, T &result)
	{
		if (minTime.GetHour() == maxTime.GetHour())
		{
			getHourlyCountminTime.GetHour()).getStatisticsResult(key, minTime, maxTime, result);
		}
		else
		{
			CDateTime start = minTime;
			int hours = maxTime.GetHour() - minTime.GetHour();
			for (int i = 0; i < hours; i++)
			{
				T ret;
				int hour = start.GetHour();
				CDateTime end = CDateTime(start.GetYear(), start.GetMonth(), start.GetDay(), hour + 1, 0, 0, 0);
				getHourlyCount(hour).getStatisticsResult(key, start, end, ret);
				start = end;
				result += ret;
			}
			T ret;
			getHourlyCount(start.GetHour()).getStatisticsResult(key, start, maxTime, ret);
			result += ret;
		}
		return 0;
	}
	MulitHourlyCount<T> &getHourlyCount(const int &hour)
	{
		std::unique_lock<std::mutex> lk(m_mutex);
		return m_daily_data[hour];
	}
private:
	std::mutex											m_mutex;
	std::unordered_map<int, MulitHourlyCount<T>>		m_daily_data;
};

template<class T>
class MulitMonthlyCount
{
public:
	int getStatisticsResult(const std::string &key, const CDateTime &minTime, const CDateTime &maxTime, T & result)
	{
		if (minTime.GetDay() == maxTime.GetDay())
		{
			getDailyCount(minTime.GetDay()).getStatisticsResult(key, minTime, maxTime, result);
		}
		else
		{
			CDateTime start = minTime;
			std::vector<std::future<T>> futures;
			int days = maxTime.GetDay() - minTime.GetDay();
			for (int i = 0; i < days; i++)
			{
				int day = start.GetDay();
				CDateTime end = CDateTime(start.GetYear(), start.GetMonth(), day, 23, 59, 59, 0);
				std::future<T> fut = std::async(std::launch::async, [&]() {
					T ret;
					getDailyCount(day).getStatisticsResult(key, start, end, ret);
					return ret;
				});
				futures.push_back(std::move(fut));
				start = CDateTime(end.GetYear(), end.GetMonth(), day + 1, 0, 0, 0, 0);
			}
			std::for_each(futures.begin(), futures.end(), [&result](std::future<T> &fut) {
				T ret = fut.get();
				result += ret;
			});
			T ret;
			getDailyCount(start.GetDay()).getStatisticsResult(key, start, maxTime, ret);
			result += ret;
		}
		return 0;
	}
	MulitDailyCount<T> &getDailyCount(const int &day)
	{
		std::unique_lock<std::mutex> lk(m_mutex);
		return m_month_data[day];
	}
private:
	std::mutex										m_mutex;
	std::unordered_map<int, MulitDailyCount<T>>     m_month_data;
};

template<class T>
class MulitYearlyCount
{
public:
	int getStatisticsResult(const std::string &key, const CDateTime &minTime, const CDateTime &maxTime, T &result)
	{
		if (minTime.GetMonth() == maxTime.GetMonth())
		{
			getMonthlyCount(minTime.GetMonth()).getStatisticsResult(key, minTime, maxTime, result);
		}
		else
		{
			CDateTime start = minTime;
			int months = maxTime.GetMonth() - minTime.GetMonth();
			for (int i = 0; i < months; i++)
			{
				T ret;
				int month = start.GetMonth();
				CDateTime end = CDateTime::getMonthEndDateTime(start);
				getMonthlyCount(month).getStatisticsResult(key, start, end, ret);
				start = CDateTime(end.GetYear(), month + 1, 1, 0, 0, 0, 0); // 进入下个月的头一天, 不会有12 月的下个月
				result += ret;
			}
			T ret;
			getMonthlyCount(start.GetMonth()).getStatisticsResult(key, start, maxTime, ret);
			result += ret;
		}
		return 0;
	}

	MulitMonthlyCount<T> &getMonthlyCount(const int &month)
	{
		std::unique_lock<std::mutex> lk(m_mutex);
		return m_year_data[month];
	}
private:
	std::mutex											m_mutex;
	std::unordered_map<int, MulitMonthlyCount<T>>		m_year_data;
};