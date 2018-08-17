from concurrent.futures import ThreadPoolExecutor
import multiprocessing
from .utils import CountUpDownLatch


def multi_processor_change_acl(adl, path=None, method_name="", acl_spec=""):
    queue_bucket_size = 10
    worker_thread_num_per_process = 50
    finish_queue_processing_flag = multiprocessing.Event()
    file_path_queue = multiprocessing.JoinableQueue()

    def launch_processes(number_of_processes):
        processes = [0 for _ in range(number_of_processes)]
        for i in range(number_of_processes):
            # TODO Instead of text and function table, check if adl method can be transferred.
            processes[i] = multiprocessing.Process(target=processor,
                                                   args=(adl, file_path_queue, finish_queue_processing_flag,
                                                         method_name, acl_spec))
            processes[i].start()
        return processes

    cpu_count = multiprocessing.cpu_count()

    # TODO Check if launching processes in a separate thread or process is better
    launch_processes(cpu_count - 1)
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

    # Cleanup
    walk_thread_pool.shutdown()
    file_path_queue.close()


def processor(adl, file_path_queue, finish_queue_processing_flag, method_name, acl_spec):
    worker_thread_num_per_process = 50
    func_table = {"mod_acl": adl.modify_acl_entries, "set_acl": adl.set_acl, "rem_acl": adl.remove_acl_entries}
    running_thread_count = CountUpDownLatch()
    function_thread_pool = ThreadPoolExecutor(max_workers=worker_thread_num_per_process)
    adl_function = func_table[method_name]

    def func_wrapper(func, path, spec):
        func(path=path, acl_spec=spec)
        running_thread_count.decrement()

    while not finish_queue_processing_flag.is_set():
        try:
            file_paths = file_path_queue.get(timeout=0.2)  # TODO Timeout value?
            file_path_queue.task_done()
            for file_path in file_paths:
                running_thread_count.increment()
                function_thread_pool.submit(func_wrapper, adl, adl_function, file_path, acl_spec)
        except Exception as e:
            # TODO Logging
            pass

    if running_thread_count.is_zero():  # Blocking call. Will wait till all threads are finished.
        pass
    function_thread_pool.shutdown()
