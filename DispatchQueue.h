#ifndef _DISPATCHQUEUE_H_
#define _DISPATCHQUEUE_H_

#include <future>
#include <queue>
#include <vector>
#include <mutex>
#include <condition_variable>

namespace Dispatch {

class DispatchQueue;

class QueueTask {
public:
    QueueTask() : _signal(false) {}

    virtual ~QueueTask() {}
    
    virtual void run() = 0;

    virtual void signal() {
        _signal = true;
        _condition.notify_all();
    }

    virtual void wait() {
        std::unique_lock <std::mutex> lock(_mutex);
        _condition.wait(lock, [this](){ return _signal; });
        _signal = false;
    }
    
    virtual void reset() {
        _signal = false;
    }

private:
    bool _signal;
    std::mutex _mutex;
    std::condition_variable _condition;
};

template <class T>
class ClosureTask : public QueueTask {
public:
    explicit ClosureTask(const T& closure) : _closure(closure) {}
private:
    void run() override {
        _closure();
    }
    T _closure;
};

class DispatchQueue {
public:
    DispatchQueue(int thread_count) {}
    
    virtual ~DispatchQueue() {}
    
    template <class T, typename std::enable_if<std::is_copy_constructible<T>::value>::type* = nullptr>
    void sync(const T &task) {
        sync(std::shared_ptr<QueueTask>(new ClosureTask<T>(task)));
    }
    
    void sync(std::shared_ptr<QueueTask> task) {
        if( nullptr != task ) {
            sync_imp(task);
        }
    }
    
    template <class T, typename std::enable_if<std::is_copy_constructible<T>::value>::type* = nullptr>
    int64_t async(const T &task) {
        return async(std::shared_ptr<QueueTask>(new ClosureTask<T>(task)));
    }
    
    int64_t async(std::shared_ptr<QueueTask> task) {
        if ( nullptr != task ) {
            return async_imp(task);
        }
        return -1;
    }
    
protected:
    virtual void sync_imp(std::shared_ptr<QueueTask> task) = 0;
    
    virtual int64_t async_imp(std::shared_ptr<QueueTask> task) = 0;
};

std::shared_ptr<DispatchQueue> create(int thread_count = 1);

}

#endif /* _DISPATCHQUEUE_H_ */
