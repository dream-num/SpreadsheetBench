"""
Univer API Inference for SpreadsheetBench
Async parallel version with slow start and auto-retry support
"""

import os
import json
import argparse
import asyncio
import time
import shutil
from typing import Dict, Any, List
from asyncio import Semaphore

# Try to import async libraries with clear error messages if missing
try:
    import aiohttp
    import aiofiles
    from tenacity import retry, stop_after_attempt, wait_exponential, RetryError
    from tqdm.asyncio import tqdm as async_tqdm
except ImportError as e:
    print(f"❌ Missing required dependencies. Please run the following command to install:")
    print(f"   pip install aiohttp aiofiles tenacity")
    print(f"   or: python -m pip install aiohttp aiofiles tenacity")
    raise


class SlowStartController:
    """Slow start concurrency controller

    Controls concurrency through semaphore and gradually increases concurrency limit
    """

    def __init__(self, max_concurrency: int, startup_interval: float = 0.5):
        """
        Args:
            max_concurrency: Maximum concurrency limit
            startup_interval: Startup interval in seconds (default: increase by 1 every 0.5s)
        """
        self.max_concurrency = max_concurrency
        self.startup_interval = startup_interval
        self.semaphore = Semaphore(1)  # Initial concurrency is 1
        self.current_limit = 1
        self._startup_task = None
        self._is_running = False

    async def start(self):
        """Start the slow start process"""
        if not self._is_running:
            self._is_running = True
            self._startup_task = asyncio.create_task(self._gradual_increase())
            print(f"🚀 Slow start initiated, initial concurrency: {self.current_limit}, max concurrency: {self.max_concurrency}")

    async def _gradual_increase(self):
        """Gradually increase concurrency limit"""
        while self.current_limit < self.max_concurrency:
            await asyncio.sleep(self.startup_interval)
            self.current_limit += 1
            # Increase semaphore capacity
            self.semaphore._value += 1
            print(f"   📈 Concurrency increased to: {self.current_limit}/{self.max_concurrency}")

        print(f"   ✅ Maximum concurrency reached: {self.max_concurrency}")

    async def acquire(self):
        """Acquire execution permission"""
        await self.semaphore.acquire()

    def release(self):
        """Release execution permission"""
        self.semaphore.release()

    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.release()

    async def stop(self):
        """Stop slow start"""
        if self._startup_task and not self._startup_task.done():
            self._startup_task.cancel()
            try:
                await self._startup_task
            except asyncio.CancelledError:
                pass


class AsyncTaskProcessor:
    """Async task processor

    Handles API calls, file downloads and other operations for individual tasks
    """

    def __init__(self, api_url: str, mock: bool = False, retry_times: int = 3):
        """
        Args:
            api_url: Univer API URL
            mock: Whether to use mock mode
            retry_times: Number of retries on failure
        """
        self.api_url = api_url
        self.mock = mock
        self.retry_times = retry_times
        self.session: aiohttp.ClientSession = None

    async def initialize(self):
        """Initialize HTTP session"""
        # Configure connection pool
        connector = aiohttp.TCPConnector(
            limit=100,  # Maximum connections
            limit_per_host=30,  # Maximum connections per host
            ttl_dns_cache=300  # DNS cache time in seconds
        )

        # Configure timeout
        timeout = aiohttp.ClientTimeout(
            total=300,  # Total timeout: 5 minutes
            connect=60,  # Connection timeout: 1 minute
            sock_read=120  # Read timeout: 2 minutes
        )

        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout
        )

        print(f"🌐 HTTP session initialized (connection pool: {connector.limit})")

    async def close(self):
        """Close session"""
        if self.session:
            await self.session.close()
            print(f"🌐 HTTP session closed")

    @retry(
        stop=stop_after_attempt(3),  # Retry up to 3 times
        wait=wait_exponential(multiplier=1, min=2, max=10),  # Exponential backoff: 2, 4, 8 seconds
        # reraise=True
    )
    async def bench_run(self, input_file_path: str, task_data: Dict[str, Any]) -> Dict:
        """Async call to Univer API (with retry mechanism)

        Args:
            input_file_path: Input file path
            task_data: Task data dictionary

        Returns:
            dict: API response
        """
        if self.mock:
            # Mock mode: simulate delay
            await asyncio.sleep(0.5)
            return {
                "error": {"code": 1, "message": "success"},
                "fileUrl": f"https://mock.univer.plus/results/{task_data['id']}_output.xlsx"
            }

        try:
            # Async read file
            async with aiofiles.open(input_file_path, 'rb') as f:
                file_content = await f.read()

            # Construct form data
            form = aiohttp.FormData()
            form.add_field(
                'file',
                file_content,
                filename=os.path.basename(input_file_path),
                content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
            form.add_field('task', json.dumps(task_data))

            # Send request
            async with self.session.post(self.api_url, data=form) as response:
                if response.status == 200:
                    result = await response.json()
                    return result
                else:
                    raise Exception(f"API returned status code: {response.status}")

        except asyncio.TimeoutError:
            raise Exception("API call timeout")
        except Exception as e:
            raise Exception(f"API call failed: {str(e)}")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        # reraise=True
    )
    async def download_file(self, file_url: str, output_path: str, input_file_path: str = None) -> bool:
        """Async file download (with retry mechanism)

        Args:
            file_url: File URL
            output_path: Output file path
            input_file_path: Input file path (used in mock mode)

        Returns:
            bool: Whether successful
        """
        if self.mock:
            # Mock mode: async copy file
            if input_file_path and os.path.exists(input_file_path):
                async with aiofiles.open(input_file_path, 'rb') as src:
                    content = await src.read()
                async with aiofiles.open(output_path, 'wb') as dst:
                    await dst.write(content)
                return True
            else:
                raise FileNotFoundError(f"Input file not found: {input_file_path}")

        try:
            async with self.session.get(file_url) as response:
                if response.status == 200:
                    async with aiofiles.open(output_path, 'wb') as f:
                        await f.write(await response.read())
                    return True
                else:
                    raise Exception(f"Download failed: status {response.status}")
        except asyncio.TimeoutError:
            raise Exception("File download timeout")
        except Exception as e:
            raise Exception(f"Download failed: {str(e)}")

    async def process_task(
        self,
        task: Dict,
        dataset_path: str,
        output_folder: str,
        test_case_idx: int = 1
    ) -> Dict:
        """Process a single task

        Args:
            task: Task data
            dataset_path: Dataset path
            output_folder: Output folder
            test_case_idx: Test case index (default: 1)

        Returns:
            dict: Processing result
        """
        task_id = task['id']
        result = {
            'id': task_id,
            'success': False,
            'error': None,
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
            'retry_count': 0
        }

        try:
            # Prepare input file
            file_name = f"{test_case_idx}_{task_id}_input.xlsx"
            input_file_path = f"{dataset_path}/{task['spreadsheet_path']}/{file_name}"

            if not os.path.exists(input_file_path):
                raise FileNotFoundError(f"Input file not found: {input_file_path}")

            # Construct task data
            task_data = {
                "id": task['id'],
                "instruction": task['instruction'],
                "instruction_type": task['instruction_type'],
                "answer_position": task['answer_position']
            }

            # Call API (with auto retry)
            try:
                api_result = await self.bench_run(input_file_path, task_data)
            except RetryError as e:
                # All retries failed
                result['retry_count'] = self.retry_times
                raise Exception(f"API call failed after {self.retry_times} retries: {str(e.last_attempt.exception())}")

            # Check API result
            if api_result.get('error', {}).get('code') != 1:
                error_msg = api_result.get('error', {}).get('message', 'Unknown error')
                raise Exception(f"API Error: {error_msg}")

            # Download result file (with auto retry)
            file_url = api_result['fileUrl']
            output_file_name = f"{test_case_idx}_{task_id}_output.xlsx"
            output_file_path = f"{output_folder}/{output_file_name}"

            try:
                await self.download_file(file_url, output_file_path, input_file_path)
            except RetryError as e:
                # All retries failed
                result['retry_count'] = self.retry_times
                raise Exception(f"File download failed after {self.retry_times} retries: {str(e.last_attempt.exception())}")

            result['success'] = True
            return result

        except Exception as e:
            result['error'] = str(e)
            return result


async def gen_solution_async(opt):
    """Async main processing function

    Args:
        opt: Command line arguments object
    """
    # Read dataset
    dataset_path = os.path.abspath(f'../data/{opt.dataset}')
    dataset_json_path = f'{dataset_path}/dataset.json'

    if not os.path.exists(dataset_json_path):
        raise FileNotFoundError(f"Dataset file not found: {dataset_json_path}")

    with open(dataset_json_path, 'r', encoding='utf-8') as fp:
        dataset = json.load(fp)

    # Create output folder
    output_folder = f'{dataset_path}/outputs/univer_{opt.model}'
    os.makedirs(output_folder, exist_ok=True)
    print(f"📁 Output folder: {output_folder}")

    # Create log folder
    log_folder = 'log'
    os.makedirs(log_folder, exist_ok=True)

    # Initialize processor and controller
    processor = AsyncTaskProcessor(opt.api_url, opt.mock, opt.retry_times)
    await processor.initialize()

    controller = SlowStartController(
        max_concurrency=opt.max_workers,
        startup_interval=opt.startup_interval
    )
    await controller.start()

    # Statistics
    stats = {
        'total': len(dataset),
        'success': 0,
        'failed': 0,
        'errors': [],
        'start_time': time.time()
    }

    # Create progress bar
    pbar = async_tqdm(
        total=len(dataset),
        desc="Processing tasks",
        unit="task",
        colour="green"
    )

    async def process_with_limit(task):
        """Task processing with concurrency limit"""
        async with controller:  # Auto acquire and release semaphore
            result = await processor.process_task(
                task, dataset_path, output_folder
            )

            # Update statistics
            if result['success']:
                stats['success'] += 1
            else:
                stats['failed'] += 1
                stats['errors'].append(result)

                # Async write error log
                log_file = f'{log_folder}/univer_{opt.model}.jsonl'
                async with aiofiles.open(log_file, 'a', encoding='utf-8') as f:
                    await f.write(json.dumps(result, ensure_ascii=False) + '\n')

            # Update progress bar
            pbar.update(1)
            success_rate = (stats['success'] / (stats['success'] + stats['failed']) * 100) if (stats['success'] + stats['failed']) > 0 else 0
            pbar.set_postfix({
                'Success': stats['success'],
                'Failed': stats['failed'],
                'Rate': f'{success_rate:.1f}%',
                'Concur': controller.current_limit
            })

            return result

    try:
        # Execute all tasks concurrently
        tasks = [process_with_limit(task) for task in dataset]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    finally:
        pbar.close()
        await controller.stop()
        await processor.close()

    # Calculate elapsed time
    elapsed_time = time.time() - stats['start_time']

    # Output statistics
    print(f"\n" + "=" * 60)
    print(f"📊 Processing Completed")
    print(f"=" * 60)
    print(f"   Total tasks: {stats['total']}")
    print(f"   Successful: {stats['success']} ({stats['success']/stats['total']*100:.1f}%)")
    print(f"   Failed: {stats['failed']} ({stats['failed']/stats['total']*100:.1f}%)")
    print(f"   Total time: {elapsed_time:.2f} seconds")
    print(f"   Avg speed: {stats['total']/elapsed_time:.2f} tasks/sec")
    print(f"   Output folder: {output_folder}")

    if stats['failed'] > 0:
        print(f"   Error log: {log_folder}/univer_{opt.model}.jsonl")

    print("=" * 60)


def gen_solution(opt):
    """Sync entry point (compatible with original calling method)"""
    asyncio.run(gen_solution_async(opt))


def parse_option():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Univer API inference for SpreadsheetBench (Async Version)"
    )

    # Basic parameters
    parser.add_argument('--model', type=str, default='univer',
                       help='Model name for output folder naming (default: univer)')
    parser.add_argument('--dataset', type=str, default="sample_data_200",
                       help='Dataset name (default: sample_data_200)')
    parser.add_argument('--api_url', type=str,
                       default="https://arena.univer.plus/arena-api/bench/run",
                       help='Univer API URL')
    parser.add_argument('--mock', action='store_true', default=True,
                       help='Use mock API (default: True)')

    # Concurrency control parameters
    parser.add_argument('--max_workers', type=int, default=20,
                       help='Maximum concurrent tasks (default: 20)')
    parser.add_argument('--startup_interval', type=float, default=0.5,
                       help='Slow start interval in seconds (default: 0.5, i.e., increase by 1 every 0.5s)')

    # Retry parameters
    parser.add_argument('--retry_times', type=int, default=3,
                       help='Number of retries on API failure (default: 3)')

    opt = parser.parse_args()
    return opt


if __name__ == '__main__':
    opt = parse_option()

    print("=" * 60)
    print("🚀 Univer Inference for SpreadsheetBench")
    print("=" * 60)
    print(f"📌 Inference Provider: {opt.model}")
    print(f"📌 Dataset: {opt.dataset}")
    print(f"📌 API URL: {opt.api_url}")
    print(f"📌 Mock mode: {'Enabled' if opt.mock else 'Disabled'}")
    print(f"📌 Max workers: {opt.max_workers}")
    print(f"📌 Startup interval: {opt.startup_interval} seconds")
    print(f"📌 Retry times: {opt.retry_times}")
    print("=" * 60)
    print()

    gen_solution(opt)
