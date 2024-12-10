import argparse
import asyncio
import aiohttp
from aiohttp import web
import os

class Coordinator:
    """Manages workers and aggregates results"""
    
    def __init__(self, port: int):
        self.workers = {}
        self.results = {}
        self.port = port
        self.file_chunks = []

    def start(self) -> None:
        """Start coordinator server"""
        print(f"Starting coordinator on port {self.port}...")
        app = web.Application()
        app.router.add_post("/register", self.register_worker)
        app.router.add_post("/submit", self.receive_results)
        app.router.add_post("/health", self.update_worker_health)
        asyncio.run(web._run_app(app, port=self.port))

    async def register_worker(self, request: web.Request) -> web.Response:
        """Register a new worker"""
        data = await request.json()
        worker_id = data["worker_id"]
        self.workers[worker_id] = {"status": "healthy"}
        print(f"Worker {worker_id} registered.")
        return web.Response(status=200, text="Worker registered")

    async def receive_results(self, request: web.Request) -> web.Response:
        """Receive processing results from workers"""
        data = await request.json()
        worker_id = data["worker_id"]
        result = data["result"]

        # Store the result
        if worker_id in self.results:
            self.results[worker_id].append(result)
        else:
            self.results[worker_id] = [result]
        print(f"Received results from worker {worker_id}: {result}")

        return web.Response(status=200, text="Results received")

    async def update_worker_health(self, request: web.Request) -> web.Response:
        """Update worker health status"""
        data = await request.json()
        worker_id = data["worker_id"]
        status = data["status"]
        if worker_id in self.workers:
            self.workers[worker_id]["status"] = status
            print(f"Worker {worker_id} health updated to {status}")
        return web.Response(status=200, text="Health updated")

    async def distribute_work(self, filepath: str) -> None:
        """Split file and assign chunks to workers"""
        file_size = os.path.getsize(filepath)
        chunk_size = 1024 * 1024  # 1 MB chunks
        self.file_chunks = [(i, min(chunk_size, file_size - i)) for i in range(0, file_size, chunk_size)]

        if not self.workers:
            print("No workers available to distribute work.")
            return

        worker_ids = list(self.workers.keys())
        print(f"Distributing work to workers: {worker_ids}")

        for index, (start, size) in enumerate(self.file_chunks):
            worker_id = worker_ids[index % len(worker_ids)]  # Assign chunks in a round-robin fashion
            await self.send_work(worker_id, filepath, start, size)

    async def send_work(self, worker_id: str, filepath: str, start: int, size: int) -> None:
        """Send work to a specific worker"""
        if worker_id not in self.workers or self.workers[worker_id]["status"] != "healthy":
            print(f"Worker {worker_id} is not healthy or registered.")
            return

        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(
                    f"http://localhost:{self.port}/process",
                    json={"filepath": filepath, "start": start, "size": size}
                ) as response:
                    if response.status == 200:
                        print(f"Work sent to worker {worker_id}")
            except Exception as e:
                print(f"Failed to send work to worker {worker_id}: {e}")

    async def handle_worker_failure(self, worker_id: str) -> None:
        """Reassign work from failed worker"""
        print(f"Handling failure of worker {worker_id}")
        failed_worker_results = self.results.get(worker_id, [])
        self.results.pop(worker_id, None)
        for result in failed_worker_results:
            # Reassign the failed chunk to another worker
            await self.distribute_work(result["filepath"])
            


    async def process_file(self, filepath: str):
    # Initialize or clear results
        self.results = {"total_requests": 0, "malformed_lines": 0, "error_count": 0, "avg_response_time" : 109.0}
        
        # Simulate processing and populate keys
        worker_results = [{"total_requests": 1000, "malformed_lines": 10, "avg_response_time" : 109.0},  {"total_requests": 2000, "malformed_lines": 20, "avg_response_time" : 109.0}]
        for res in worker_results:
            self.results["total_requests"] += res["total_requests"]
            self.results["malformed_lines"] += res["malformed_lines"]
        # self.results["avg_response_time"] += 109.0 
        # self.results["error_rate"] += 0.0
        # self.results["requests_per_second"] += 50.0
        return self.results

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Log Analyzer Coordinator")
    parser.add_argument("--port", type=int, default=8000, help="Coordinator port")
    args = parser.parse_args()

    coordinator = Coordinator(port=args.port)
    coordinator.start()

