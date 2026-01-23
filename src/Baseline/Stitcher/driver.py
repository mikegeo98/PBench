import functools
import multiprocessing
import os
import re
import time

from databend_py import Client


def execute(driver):
    """  Executes the SQL queries at a specified frequency for a given duration. """
    client = Client(f"root:@{driver.host}", port=driver.port, secure=False, database=driver.database)
    start_time = time.time()
    interval = int(driver.time_slot * 0.9 / driver.frequency)
    idx = -1
    cnt = 0

    while True:
        begin = time.time()
        idx = (idx + 1) % len(driver.queries)
        query = driver.queries[idx]
        
        query = query.split(";")
        if query[0] == "":
            query = query[1:]
        # make sure the last query is not empty
        if query[-1] == "":
            query = query[:-1]
        # add the last semicolon to each query
        query = [q + ";" for q in query]
        ret = []
        for q in query:
            if not q.startswith("Explain Analyze") and not q.startswith("EXPLAIN ANALYZE"):
                q = "Explain Analyze " + q
            print(q)
            ret.append(client.execute(q))

        cnt += 1
        diff = time.time() - begin
        elapsed_time = time.time() - start_time
        
        if elapsed_time >= driver.time_slot or cnt >= driver.frequency:
            print(f"{driver.benchmark}: {cnt} queries executed ...")
            break
        
        time.sleep(max(0, interval - diff))


class BenchmarkDriver:
    """ A driver class for running benchmark tests on a database. """
    def __init__(self, host, port, time_slot, benchmark, database, frequency):
        self.host = host
        self.port = port
        self.time_slot = time_slot
        self.benchmark = benchmark
        self.database = database
        self.frequency = frequency
        self.queries = self._load_queries(benchmark)
        self.process = None

    def _load_queries(self, benchmark):
        """ Loads and sorts SQL queries from a file. """
        queries = []
        current_statement = ""
        sort_key = None
        file_path = os.path.join(os.path.dirname(__file__), "benchmark", f"{benchmark}.sql")
        with open(file_path, "r") as file:
            for line in file:
                if line.startswith(" '.SQL/"):
                    if current_statement:
                        queries.append((sort_key, current_statement.strip()))
                        current_statement = ""
                    sort_key = int(re.search(r"\.SQL/(\d+)\.0", line).group(1))
                else:
                    current_statement += line.strip() + " "
        if current_statement:
            queries.append((sort_key, current_statement.strip()))
        queries = queries[1:]
        return [stmt[1] for stmt in queries]


    def run(self):
        """ Starts the execution of SQL queries in a separate process. """
        self.process = multiprocessing.Process(target=functools.partial(execute, self))
        self.process.start()


    def terminate(self):
        self.process.terminate()


    def wait(self):
        """ Waits for the process executing the SQL queries to complete. """
        self.process.join()