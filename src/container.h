#pragma once
#include <list>
#include <mutex>
#include <memory>
#include <vector>
#include <unordered_map>
#include <utility>
#include <atomic>
#include "boost/thread/mutex.hpp"
#include "boost/thread/lock_guard.hpp"
#include "boost/thread/shared_mutex.hpp"

using namespace std;
template<typename Key, typename Value, int n = 199, typename Hash = std::hash<Key>>
class threadsafe_lookup_table // 只针对每个bucket上锁，而全局的vector不上锁，从而降低了锁的粒度。若使用map则是简单的操作都需要对整个map上锁,频繁读写可用该容器
{
private:
	class bucket_type
	{
	private:
		typedef std::unordered_map<Key, Value> bucket_data;
		typedef typename bucket_data::iterator bucket_iterator;
		typedef typename bucket_data::const_iterator bucket_const_iterator;

		bucket_data data;
		mutable boost::shared_mutex _mutex;

		bucket_iterator find_entry_for(Key const& key)
		{
			return data.find(key);
		}

		bucket_const_iterator find_entry_for(Key const& key) const //由于要返回非const的迭代器所以将数据放置到非const变量返回
		{
			return data.find(key);
		}
	public:

		Value value_for(Key const& key, Value const& default_value) const  //在这个函数读取时将锁锁定
		{
			boost::shared_lock<boost::shared_mutex> lock(_mutex);
			bucket_const_iterator found_entry = find_entry_for(key);
			return (found_entry == data.end()) ? default_value : found_entry->second;
		}

		void add_or_update_mapping(Key const& key, Value const& value) //在修改时也将锁锁定
		{
			boost::unique_lock<boost::shared_mutex> lock(_mutex);
			bucket_iterator found_entry = find_entry_for(key);
			if (found_entry == data.end())
			{
				data[key] = value;
			}
			else
			{
				found_entry->second = value; //如果key已经存在就更新value
			}
		}

		void remove_mapping(Key const& key)
		{
			boost::unique_lock<boost::shared_mutex> lock(_mutex);
			bucket_iterator const found_entry = find_entry_for(key);
			if (found_entry != data.end())
			{
				data.erase(found_entry);
			}
		}

		template<typename Predicate>
		void remove_if(Predicate &&p)
		{
			boost::unique_lock<boost::shared_mutex> lock(_mutex);
			for (bucket_iterator it = data.begin(); it != data.end();)
			{
				if (p(it->second)) it = data.erase(it);
				else ++it;
			}
		}

		template<typename Function>
		void for_each(Function &&f)
		{
			boost::unique_lock<boost::shared_mutex> lock(_mutex);
			for (bucket_iterator it = data.begin(); it != data.end(); it++)
			{
				f(it->second);
			}
		}
	};

	std::vector<std::unique_ptr<bucket_type> > buckets; //持有桶的变量
	Hash hasher;

	bucket_type& get_bucket(Key const& key) const  //获取数量并不改变所以不需要锁
	{
		std::size_t const bucket_index = hasher(key) % buckets.size();
		return *buckets[bucket_index];
	}

public:
	threadsafe_lookup_table(
		unsigned num_buckets = n, Hash const& hasher_ = Hash()) :
		buckets(num_buckets), hasher(hasher_) //初始化数量，质数与哈希效率最高所以选择29初始化
	{
		for (unsigned i = 0; i < num_buckets; ++i)
		{
			buckets[i].reset(new bucket_type);
		}
	}

	threadsafe_lookup_table(threadsafe_lookup_table const& other) = delete;
	threadsafe_lookup_table const& operator=(threadsafe_lookup_table const& other) = delete;//为了简化代码禁止拷贝和赋值

	Value value_for(Key const& key, Value const& default_value = Value())
	{
		return get_bucket(key).value_for(key, default_value);
	}

	void add_or_update_mapping(Key const& key, Value const& value)//异常安全的，因为add_or_update_mapping是异常安全的
	{
		get_bucket(key).add_or_update_mapping(key, value);
	}

	void remove_mapping(Key const& key)
	{
		get_bucket(key).remove_mapping(key);
	}

	template<typename Predicate>
	void remove_if(Predicate &&p)
	{
		for (unsigned i = 0; i < buckets.size(); ++i)
		{
			(*buckets[i]).remove_if(std::forward<Predicate>(p));
		}
	}

	template<typename Function>
	void for_each(Function &&f)//针对map中每个元素执行f
	{
		for (unsigned i = 0; i < buckets.size(); ++i)
		{
			(*buckets[i]).for_each(std::forward<Function>(f));
		}
	}
};


template <typename T>
class FreeList
{
public:
	FreeList() {}
	~FreeList()
	{
		m_mutex.lock();
		for (typename std::list<T*>::iterator it = m_list.begin(); it != m_list.end();)
		{
			if (*it) delete *it;
			it = m_list.erase(it);
		}
		m_mutex.unlock();
	}
	void initSize(int n)
	{
		m_mutex.lock();
		for (int i = 0; i<n; i++)
		{
			T *  t = new T();
			m_list.push_back(t);
		}
		m_mutex.unlock();
	}
	T *getFreeElem()
	{
		T *t = NULL;
		m_mutex.lock();
		if (!m_list.empty())
		{
			t = m_list.front();
			m_list.pop_front();
		}
		else t = new T();
		m_mutex.unlock();
		return t;
	}
	void putFreeElem(T * t)
	{
		m_mutex.lock();
		m_list.push_front(t);
		m_mutex.unlock();
	}
private:

	std::list<T *> m_list;
	std::mutex m_mutex;
};


template<typename T>
class threadsafe_list  // 频繁写建议使用这个容器
{
	struct node//每个节点持有一个mutex
	{
		boost::mutex m;
		std::shared_ptr<T> data;
		std::unique_ptr<node> next;

		node() :next(){}

		node(shared_ptr<T> const &value)
		{
			data = value;
		}
	};

	node head;
public:
	threadsafe_list()
	{}

	~threadsafe_list()
	{
		remove_if([](shared_ptr<T> const&) {return true; });
	}

	threadsafe_list(threadsafe_list const& other) = delete;
	threadsafe_list& operator=(threadsafe_list const& other) = delete;

	void push_front(shared_ptr<T> const& value)//从头部插入一个节点只需要锁住head
	{
		std::unique_ptr<node> new_node(new node(value));//在临界区外new，这样既可以减小临界区又可以避免临界区中抛出异常
		boost::lock_guard<boost::mutex> lk(head.m);
		new_node->next = std::move(head.next);//unique_ptr不能直接赋值，但可以通过reset或move
		head.next = std::move(new_node);
	}

	void push_back(shared_ptr<T>  const& value)
	{
		node* current = &head;
		boost::unique_lock<boost::mutex> lk(head.m);
		while (node* const next = current->next.get())  // 移动到最后一个
		{
			boost::unique_lock<boost::mutex> next_lk(next->m);
			lk.unlock();//拿到当前元素的锁后立即释放上一个锁
			current = next;
			lk = std::move(next_lk);
		}
		std::unique_ptr<node> new_node(new node(value));
		new_node->next = std::move(current->next);
		current->next = std::move(new_node);
	}

	std::shared_ptr<T> pop_front()//从头部获取一个节点
	{
		node* current = &head;
		boost::unique_lock<boost::mutex> lk(head.m);
		if (node* const next = current->next.get())
		{
			boost::unique_lock<boost::mutex> next_lk(next->m);
			std::unique_ptr<node> old_next = std::move(current->next);
			current->next = std::move(next->next);//重置连接
			next_lk.unlock();
			return next->data;
		}
		return std::shared_ptr<T>();
	}

	template<typename Function>
	void for_each(Function &&f)//针对链表中每个元素执行f
	{
		node* current = &head;
		boost::unique_lock<boost::mutex> lk(head.m);
		while (node* const next = current->next.get())
		{
			boost::unique_lock<boost::mutex> next_lk(next->m);//锁住当前节点后，立即释放上一个节点
			lk.unlock();//
			f(next->data.get());
			current = next;
			lk = std::move(next_lk);//向后移动，unique_lock is moveable not copyable，而lock_guard不具备移动语义，可见unique_lock比lock_guard灵活
		}
	}

	void remove(shared_ptr<T>  const& value)
	{
		node* current = &head;
		boost::unique_lock<boost::mutex> lk(head.m);
		while (node* const next = current->next.get())
		{
			boost::unique_lock<boost::mutex> next_lk(next->m);
			if (next->data == value)
			{
				std::unique_ptr<node> old_next = std::move(current->next);
				current->next = std::move(next->next);//重置连接
				next_lk.unlock();//注意这里并没有对lk解锁或者重置
				break;
			}
			else
			{
				lk.unlock();
				current = next;
				lk = std::move(next_lk);
			}
		}
	}

	template<typename Predicate>
	void remove_if(Predicate p)//删除哪些使得谓词P返回true的元素
	{
		node* current = &head;
		boost::unique_lock<boost::mutex> lk(head.m);
		while (node* const next = current->next.get())
		{
			boost::unique_lock<boost::mutex> next_lk(next->m);
			if (p(next->data))
			{
				std::unique_ptr<node> old_next = std::move(current->next);
				current->next = std::move(next->next);//重置连接
				next_lk.unlock();//注意这里并没有对lk解锁或者重置
			}
			else
			{
				lk.unlock();
				current = next;
				lk = std::move(next_lk);
			}
		}
	}

	void remove_all()
	{
		remove_if([](shared_ptr<T> const&) {return true; });
	}
};


