#include "DispatchQueue.h"
#include <disruptor/ring_buffer.h>
#include <disruptor/sequencer.h>
#include <iostream>

namespace Dispatch {
    
using kClaimStrategy = disruptor::MultiThreadedStrategy<disruptor::kDefaultRingBufferSize>;
using kWaitStrategy = disruptor::SleepingStrategy<>;
using DisruptorSequencer = disruptor::Sequencer<std::shared_ptr<QueueTask>, disruptor::kDefaultRingBufferSize, kClaimStrategy, kWaitStrategy>;
    
class MutexQueueImp : public DispatchQueue {
public:
    MutexQueueImp(int thread_count) : DispatchQueue(thread_count),
        _cancel(false),
        _thread_count(thread_count)
    {
        for ( int i = 0; i < thread_count; i++ ) 
        {
            create_thread();
        }
    }

    ~MutexQueueImp()
    {
        _cancel = true;
        _condition.notify_all();
        for ( auto& future : _futures )
        {
            future.wait();
        }
    }
    
    void sync_imp(std::shared_ptr<QueueTask> task) override
    {
        if ( _thread_count == 1 && _thread_id == std::this_thread::get_id() )
        {
            task->reset();
            task->run();
            task->signal();
        }
        else
        {
            async_imp(task);
            task->wait();
        }
    }
    
    int64_t async_imp(std::shared_ptr<QueueTask> task) override 
    {
        _mutex.lock();
        task->reset();
        _dispatch_tasks.push(task);
        _mutex.unlock();
        _condition.notify_one();
        return 0;
    }
    
private:
    MutexQueueImp(const MutexQueueImp&);
    
    void create_thread() 
    {
        _futures.emplace_back(std::async(std::launch::async, [&]() 
        {
            _thread_id = std::this_thread::get_id();
            while (!_cancel) 
            {
                {
                    std::unique_lock <std::mutex> work_signal(_signal_mutex);
                    _condition.wait(work_signal, [this](){
                        return _cancel || !_dispatch_tasks.empty();
                    });
                }
                
                while (!_dispatch_tasks.empty() && !_cancel) 
                {
                    _mutex.lock();
                    if ( _dispatch_tasks.empty() ) 
                    {
                        _mutex.unlock();
                        break;
                    }
                    std::shared_ptr<QueueTask> task(_dispatch_tasks.front());
                    _dispatch_tasks.pop();
                    _mutex.unlock();
                    if ( nullptr != task ) 
                    {
                        task->run();
                        task->signal();
                    }
                }
            }
        }));
    }
    
private:
    std::vector<std::future<void>> _futures;
    std::queue<std::shared_ptr<QueueTask>> _dispatch_tasks;
    std::recursive_mutex _mutex;
    bool _cancel;
    std::mutex _signal_mutex;
    std::condition_variable _condition;
    int _thread_count;
    std::thread::id _thread_id;
};

class DisruptorImp : public DispatchQueue {
    
public:
    DisruptorImp() : DispatchQueue(1), _cancel(false)
    {
        _sequencer = new DisruptorSequencer(_calls);
        create_thread();
    }
    
    ~DisruptorImp()
    {
        _cancel = true;
        int64_t seq = _sequencer->Claim();
        (*_sequencer)[seq] = nullptr;
        _sequencer->Publish(seq);
        _future.wait();
        delete _sequencer;
    }
    
private:
    DisruptorImp(const DisruptorImp&);
    
    void sync_imp(std::shared_ptr<QueueTask> task) override 
    {
        if ( _thread_id == std::this_thread::get_id() ) 
        {
            task->reset();
            task->run();
            task->signal();
        }
        else
        {
            async_imp(task);
            task->wait();
        }
    }
    
    int64_t async_imp(std::shared_ptr<QueueTask> task) override 
    {
        task->reset();
        int64_t seq = _sequencer->Claim();
        (*_sequencer)[seq] = task;
        _sequencer->Publish(seq);
        return 0;
    }
    
    void create_thread() 
    {
        _future = std::async(std::launch::async, [&]() 
        {
            _thread_id = std::this_thread::get_id();
            this->run();
        });
    }
    
    void run() 
    {
        int64_t seqWant(disruptor::kFirstSequenceValue);
        int64_t seqGeted, i;
        std::vector<disruptor::Sequence*> dependents;
        disruptor::SequenceBarrier<kWaitStrategy>* barrier;
        
        disruptor::Sequence handled(disruptor::kInitialCursorValue);
        dependents.push_back(&handled);
        _sequencer->set_gating_sequences(dependents);
        dependents.clear();
        barrier = _sequencer->NewBarrier(dependents);
        
        while (!_cancel)
        {
            seqGeted = barrier->WaitFor(seqWant);
            for (i = seqWant; i <= seqGeted; i++)
            {
                std::shared_ptr<QueueTask> task((*_sequencer)[i]);
                (*_sequencer)[i] = nullptr;
                handled.set_sequence(i);
                if ( nullptr != task )
                {
                    task->run();
                    task->signal();
                }
            }
            seqWant = seqGeted + 1;
        }
        
        delete barrier;
    }
    
private:
    DisruptorSequencer *_sequencer;
    std::array<std::shared_ptr<QueueTask>, disruptor::kDefaultRingBufferSize> _calls;
    bool _cancel;
    std::future<void> _future;
    std::thread::id _thread_id;
};

std::shared_ptr<DispatchQueue> create(int thread_count)
{
    if ( 1 == thread_count )
    {
        return std::shared_ptr<DispatchQueue>(new DisruptorImp());
    }
    else
    {
        return std::shared_ptr<DispatchQueue>(new MutexQueueImp(thread_count));
    }
}

} // namespace dispatch
