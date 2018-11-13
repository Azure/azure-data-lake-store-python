from concurrent.futures import ThreadPoolExecutor
from .utils import CountUpDownLatch
import threading
import logging
import multiprocessing
import os
import logging.handlers
from .exceptions import  FileNotFoundError


try:
    from queue import Empty     # Python 3
except ImportError:
    from Queue import Empty     # Python 2
end_queue_sentinel = [None, None]

exception = None
exception_lock = threading.Lock()


threading
def monitor_exception(exception_queue, process_ids):
    global exception
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    while True:
        try:
            excep = exception_queue.get(timeout=0.1)
            if excep == end_queue_sentinel:
                break
            logger.log(logging.DEBUG, "Setting global exception")
            exception_lock.acquire()
            exception = excep
            exception_lock.release()
            logger.log(logging.DEBUG, "Closing processes")
            for p in process_ids:
                p.terminate()
            logger.log(logging.DEBUG, "Joining processes")
            for p in process_ids:
                p.join()
            import thread
            logger.log(logging.DEBUG, "Interrupting main")
            raise Exception(excep)
        except Empty:
            pass


def log_listener_process(queue):
    while True:
        try:
            record = queue.get(timeout=0.1)
            queue.task_done()
            if record == end_queue_sentinel:  # We send this as a sentinel to tell the listener to quit.
                break
            logger = logging.getLogger(record.name)
            logger.handlers.clear()
            logger.handle(record)  # No level or filter logic applied - just do it!
        except Empty:               # Try again
            pass
        except Exception as e:
            import sys, traceback
            print('Problems in logging')
            traceback.print_exc(file=sys.stderr)


def multi_processor_change_acl(adl, path=None, method_name="", acl_spec="", number_of_sub_process=None):
    log_queue = multiprocessing.JoinableQueue()
    exception_queue = multiprocessing.Queue()
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    queue_bucket_size = 10
    worker_thread_num_per_process = 50

    def launch_processes(number_of_processes):
        process_list = []
        for i in range(number_of_processes):
            process_list.append(multiprocessing.Process(target=processor,
                                    args=(adl, file_path_queue, finish_queue_processing_flag,
                                          method_name, acl_spec, log_queue, exception_queue)))
            process_list[-1].start()
        return process_list

    def walk(walk_path):
        try:
            paths = []
            all_files = adl._ls(path=walk_path)

            for files in all_files:
                if files['type'] == 'DIRECTORY':
                    dir_processed_counter.increment()               # A new directory to process
                    walk_thread_pool.submit(walk, files['name'])
                paths.append(files['name'])
                if len(paths) == queue_bucket_size:
                    file_path_queue.put(list(paths))
                    paths = []
            if paths != []:
                file_path_queue.put(list(paths))  # For leftover paths < bucket_size
        except FileNotFoundError:
            pass                    # Continue in case the file was deleted in between
        except:
            import traceback
            logger.exception("Failed to walk for path: " + str(walk_path) + ". Exiting!")
            exception_queue.put(traceback.format_exc())
        finally:
            dir_processed_counter.decrement()           # Processing complete for this directory

    finish_queue_processing_flag = multiprocessing.Event()
    file_path_queue = multiprocessing.JoinableQueue()
    if number_of_sub_process == None:
        number_of_sub_process = max(2, multiprocessing.cpu_count()-1)

    child_processes = launch_processes(number_of_sub_process)
    exception_monitor_thread = threading.Thread(target=monitor_exception, args=(exception_queue, child_processes))
    exception_monitor_thread.start()
    log_listener = threading.Thread(target=log_listener_process, args=(log_queue,))
    log_listener.start()

    dir_processed_counter = CountUpDownLatch()
    walk_thread_pool = ThreadPoolExecutor(max_workers=worker_thread_num_per_process)

    file_path_queue.put([path])  # Root directory needs to be passed
    dir_processed_counter.increment()
    walk(path)                  # Start processing root directory

    if dir_processed_counter.is_zero(): # Done processing all directories. Blocking call.
        walk_thread_pool.shutdown()
        file_path_queue.close()          # No new elements to add
        file_path_queue.join()           # Wait for operations to be done
        logger.log(logging.DEBUG, "file path queue closed")
        finish_queue_processing_flag.set()  # Set flag to break loop of child processes
        for child in child_processes:  # Wait for all child process to finish
            logger.log(logging.DEBUG, "Joining process: "+str(child.pid))
            child.join()

    # Cleanup
    logger.log(logging.DEBUG, "Sending exception sentinel")
    exception_queue.put(end_queue_sentinel)
    exception_monitor_thread.join()
    logger.log(logging.DEBUG, "Exception monitor thread finished")
    logger.log(logging.DEBUG, "Sending logger sentinel")
    log_queue.put(end_queue_sentinel)
    log_queue.join()
    log_queue.close()
    logger.log(logging.DEBUG, "Log queue closed")
    log_listener.join()
    logger.log(logging.DEBUG, "Log thread finished")


def processor(adl, file_path_queue, finish_queue_processing_flag, method_name, acl_spec, log_queue, exception_queue):
    logger = logging.getLogger(__name__)

    try:
        logger.addHandler(logging.handlers.QueueHandler(log_queue))
        logger.propagate = False                                                        # Prevents double logging
    except AttributeError:
        # Python 2 doesn't have Queue Handler. Default to best effort logging.
        pass
    logger.setLevel(logging.DEBUG)

    try:
        worker_thread_num_per_process = 50
        func_table = {"mod_acl": adl.modify_acl_entries, "set_acl": adl.set_acl, "rem_acl": adl.remove_acl_entries}
        function_thread_pool = ThreadPoolExecutor(max_workers=worker_thread_num_per_process)
        adl_function = func_table[method_name]
        logger.log(logging.DEBUG, "Started processor pid:"+str(os.getpid()))

        def func_wrapper(func, path, spec):
            try:
                func(path=path, acl_spec=spec)
            except FileNotFoundError as e:
                logger.exception("File "+str(path)+" not found")
                pass    # Exception is being logged in the relevant acl method. Do nothing here
            except:
                # TODO Raise to parent process
                pass

            logger.log(logging.DEBUG, "Completed running on path:" + str(path))

        while finish_queue_processing_flag.is_set() == False:
            try:
                file_paths = file_path_queue.get(timeout=0.1)
                file_path_queue.task_done()                 # Will not be called if empty
                for file_path in file_paths:
                    logger.log(logging.DEBUG, "Starting on path:" + str(file_path))
                    function_thread_pool.submit(func_wrapper, adl_function, file_path, acl_spec)
            except Empty:
                pass

    except Exception as e:
        import traceback
        # TODO Raise to parent process
        logger.exception("Exception in pid "+str(os.getpid())+"Exception: " + str(e))
        exception_queue.put(traceback.format_exc())
    finally:
        function_thread_pool.shutdown()  # Blocking call. Will wait till all threads are done executing.
        logger.log(logging.DEBUG, "Finished processor pid: " + str(os.getpid()))
