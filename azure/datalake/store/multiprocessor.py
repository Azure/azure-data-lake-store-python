from concurrent.futures import ThreadPoolExecutor
from .utils import CountUpDownLatch
import threading
import logging
import multiprocessing
import logging.handlers
from  queue import Empty
import time


def listener_process(queue):
    print("Setting up logger listener")
    while True:
        try:
            record = queue.get(timeout=0.1)
            queue.task_done()
            if record == [None, None]:  # We send this as a sentinel to tell the listener to quit.
                break
            logger = logging.getLogger(record.name)
            logger.handle(record)  # No level or filter logic applied - just do it!
        except Empty:
            pass
        except Exception:
            import sys, traceback
            print('Problem in logging:', file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
    print("Exiting logger process")


def multi_processor_change_acl(adl, path=None, method_name="", acl_spec=""):
    log_queue = multiprocessing.JoinableQueue()
    log_listener = threading.Thread(target=listener_process, args=(log_queue,))
    log_listener.start()
    print(threading.enumerate())
    queue_bucket_size = 10
    worker_thread_num_per_process = 50
    finish_queue_processing_flag = multiprocessing.Event()
    file_path_queue = multiprocessing.JoinableQueue()

    def launch_processes(number_of_processes):
        process_list = []
        for i in range(number_of_processes):
            process_list.append(multiprocessing.Process(target=processor,
                                    args=(adl, file_path_queue, finish_queue_processing_flag,
                                          method_name, acl_spec, log_queue)))
            process_list[-1].start()
        return process_list
    cpu_count = multiprocessing.cpu_count()
    child_process = launch_processes(1)
    dir_processed = CountUpDownLatch()
    walk_thread_pool = ThreadPoolExecutor(max_workers=worker_thread_num_per_process)

    def walk(walk_path):
        paths = []
        all_files = adl.ls(path=walk_path, detail=True)
        for files in all_files:
            if files['type'] == 'DIRECTORY':
                dir_processed.increment()  # A new directory to process
                walk_thread_pool.submit(walk, files['name'])
            paths.append(files['name'])
            if len(paths) == queue_bucket_size:
                file_path_queue.put(list(paths))
                paths = []

        file_path_queue.put(list(paths))  # For leftover paths < bucket_size
        dir_processed.decrement()  # Processing complete for this directory

    file_path_queue.put([path])  # Root directory

    dir_processed.increment()  # Start processing root directory
    walk(path)

    if dir_processed.is_zero():  # Done processing all directories. Blocking call.
        file_path_queue.join()  # Wait for all queue items to be done
        finish_queue_processing_flag.set()  # Set flag to break loop of child processes
        for child in child_process: # Wait for all child process to finish
            child.join()

    # Cleanup
    print("Sending logger sentinel")
    log_queue.put([None, None])
    log_queue.join()
    print("Joined")
    log_queue.close()
    print("Joining log listener")
    log_listener.join()
    print("Walk thread pool shutdown")
    walk_thread_pool.shutdown()
    print("File path queue shutdown")
    file_path_queue.close()
    #process_start_thread.join()


def processor_log_configurer(queue):
    h = logging.handlers.QueueHandler(queue)  # Just the one handler needed
    root = logging.getLogger()
    root.addHandler(h)
    # send all messages, for demo; no other level or filter logic applied.
    root.setLevel(logging.DEBUG)

def processor(adl, file_path_queue, finish_queue_processing_flag, method_name, acl_spec, log_queue):

    logger = logging.getLogger(__name__)
#    processor_log_configurer(log_queue)
    logger.addHandler(logging.handlers.QueueHandler(log_queue))
    logger.setLevel(logging.DEBUG)

    worker_thread_num_per_process = 50
    func_table = {"mod_acl": adl.modify_acl_entries, "set_acl": adl.set_acl, "rem_acl": adl.remove_acl_entries}
    running_thread_count = CountUpDownLatch()
    function_thread_pool = ThreadPoolExecutor(max_workers=worker_thread_num_per_process)
    adl_function = func_table[method_name]
    logger.log(logging.DEBUG, "Started processor")
    def func_wrapper(func, path, spec):
        func(path=path, acl_spec=spec)
        running_thread_count.decrement()
        logger.log(logging.DEBUG, "Finished Running on file " + path)

    while not finish_queue_processing_flag.is_set():
        try:
            file_paths = file_path_queue.get(timeout=0.2)  # TODO Timeout value?
            file_path_queue.task_done()
            for file_path in file_paths:
                running_thread_count.increment()
                logger.log(logging.DEBUG, "Running on file " + file_path)
                function_thread_pool.submit(func_wrapper, adl_function, file_path, acl_spec)
        except Empty:
            pass
        except Exception as e:
            # TODO Logging
            # Multiprocessor complicates things
            logger.log(logging.DEBUG, "Exception = "+str(e))

    logger.log(logging.DEBUG, "Finishing processor work")
    if running_thread_count.is_zero():  # Blocking call. Will wait till all threads are finished.
        pass
    logger.log(logging.DEBUG, "Shuttding down function thread pool")
    function_thread_pool.shutdown()
