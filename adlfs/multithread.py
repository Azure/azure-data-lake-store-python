"""
High performance multi-threaded module to up/download

Calls method in `core` with thread pool executor to ensure the network
is used to its maximum throughput.

Only implements upload and download of (massive) files and directory trees.
"""
