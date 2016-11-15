#include <memory>
#include <mutex>

template<typename T>
class ThreadsafeQueue {
private:
  struct Node {
    std::shared_ptr<T> data;
    std::unique_ptr<Node> next;
  };

  std::mutex head_mutex;
  std::unique_ptr<Node> head;
  std::mutex tail_mutex;
  Node *tail;
  std::mutex end_mutex;
  bool End;

  // Helper functions
  Node *getTail() {
    std::lock_guard<std::mutex> tail_lock(tail_mutex);
    return tail;
  }

  std::unique_ptr<Node> popHead() {
    std::lock_guard<std::mutex> head_lock(head_mutex);
    if (head.get() == getTail())
      return nullptr;

    std::unique_ptr<Node> OldHead = std::move(head);
    head = std::move(OldHead->next);
    return OldHead;
  }

public:
  
  ThreadsafeQueue() : head(new Node), tail(head.get()),
                      End(false) {}

  ThreadsafeQueue(const ThreadsafeQueue &other) = delete;
  ThreadsafeQueue &operator =(const ThreadsafeQueue &other) = delete;

  std::shared_ptr<T> tryPop() {
    std::unique_ptr<Node> oldHead = popHead();
    return oldHead ? oldHead->data : std::shared_ptr<T>();
  }

  void push(T &NewValue) {
    std::shared_ptr<T>
      NewData(std::make_shared<T>(std::move(NewValue)));
    std::unique_ptr<Node> p(new Node);
    Node *NewTail = p.get();
    std::lock_guard<std::mutex> tail_lock(tail_mutex);
    tail->data = NewData;
    tail->next = std::move(p);
    tail = NewTail;
  }

  void setEnd() {
    std::lock_guard<std::mutex> end_lock(end_mutex);
    End = true;
  }

  bool getEnd() {
    std::lock_guard<std::mutex> end_lock(end_mutex);
    return End;
  }
};
