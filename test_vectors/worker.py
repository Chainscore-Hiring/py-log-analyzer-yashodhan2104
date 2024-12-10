import argparse
import aiohttp
import asyncio
import os
from aiohttp import web

class Worker:
    """Processes log chunks and reports results"""
    
    def __init__(self, port: int, worker_id: str, coordinator_url: str):
        self.worker_id = worker_id
        self.coordinator_url = coordinator_url
        self.port = port

    def start(self) -> None:
        """Start worker server"""
        print(f"Starting worker {self.worker_id} on port {self.port}...")
        app = web.Application()
        app.router.add_post("/process", self.process_request)
        asyncio.ensure_future(self.report_health())  
        asyncio.run(web._run_app(app, port=self.port))

    async def process_request(self, request: web.Request) -> web.Response:
        """Handle chunk processing requests from the coordinator"""
        data = await request.json()
        filepath = data["filepath"]
        start = data["start"]
        size = data["size"]

        print(f"Processing chunk: start={start}, size={size}")
        result = await self.process_chunk(filepath, start, size)
        print(f"Processed chunk: {result}")


        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(
                    f"{self.coordinator_url}/submit",
                    json={"worker_id": self.worker_id, "result": result}
                ) as response:
                    if response.status == 200:
                        print("Result submitted successfully")
            except Exception as e:
                print(f"Failed to submit result: {e}")

        return web.Response(status=200, text="Chunk processed")

    async def process_chunk(self, filepath: str, start: int, size: int) -> dict:
        """Process a chunk of log file and return metrics"""
        metrics = {"lines": 0, "errors": 0, "warnings": 0}
        try:
            with open(filepath, "r") as file:
                file.seek(start)
                data = file.read(size)
                lines = data.splitlines()
                metrics["lines"] = len(lines)
                metrics["errors"] = sum(1 for line in lines if "ERROR" in line)
                metrics["warnings"] = sum(1 for line in lines if "WARNING" in line)
        except Exception as e:
            print(f"Error processing chunk: {e}")
        return metrics

    async def report_health(self) -> None:
        """Send heartbeat to coordinator"""
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        f"{self.coordinator_url}/health",
                        json={"worker_id": self.worker_id, "status": "healthy"}
                    ) as response:
                        if response.status == 200:
                            print(f"Health check sent successfully")
            except Exception as e:
                print(f"Failed to send health check: {e}")
            await asyncio.sleep(10)  


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Log Analyzer Worker")
    parser.add_argument("--port", type=int, default=8001, help="Worker port")
    parser.add_argument("--id", type=str, default="worker1", help="Worker ID")
    parser.add_argument("--coordinator", type=str, default="http://localhost:8000", help="Coordinator URL")
    args = parser.parse_args()

    worker = Worker(port=args.port, worker_id=args.id, coordinator_url=args.coordinator)
    worker.start()

