import threading
import queue
import time

class Job:
    def __init__(self, fn, args=None, kwargs=None, priority=0, description=None):
        self.fn = fn
        self.args = args or ()
        self.kwargs = kwargs or {}
        self.priority = priority
        self.description = description

    def run(self):
        return self.fn(*self.args, **self.kwargs)

class JobQueue:
    def __init__(self):
        self.q = queue.PriorityQueue()
        self.shutdown_flag = threading.Event()
        self.worker_thread = threading.Thread(target=self.worker, daemon=True)

    def start(self):
        self.worker_thread.start()

    def enqueue(self, job):
        # Lower priority number = higher priority
        self.q.put((job.priority, time.time(), job))

    def worker(self):
        while not self.shutdown_flag.is_set():
            try:
                priority, _, job = self.q.get(timeout=1)
                if job.description:
                    print(f"Running job: {job.description}")
                job.run()
            except queue.Empty:
                continue
            except Exception as e:
                print("Job error:", e)

    def stop(self):
        self.shutdown_flag.set()
        self.worker_thread.join()
