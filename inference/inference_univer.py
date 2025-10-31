"""
Univer API Inference for SpreadsheetBench
Async parallel version with slow start and auto-retry support
"""

import os
import json
import argparse
import asyncio
import time
from typing import Dict, Any
from asyncio import Semaphore
from pathlib import Path

# Try to import async libraries with clear error messages if missing
try:
    import httpx
    import aiofiles
    from tenacity import retry, stop_after_attempt, wait_exponential, RetryError
    from tqdm.asyncio import tqdm as async_tqdm
except ImportError as e:
    print(f"❌ Missing required dependencies. Please run the following command to install:")
    print(f"   pip install httpx aiofiles tenacity")
    print(f"   or: python -m pip install httpx aiofiles tenacity")
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
            # print(f"   📈 Concurrency increased to: {self.current_limit}/{self.max_concurrency}")

        # print(f"   ✅ Maximum concurrency reached: {self.max_concurrency}")

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
        self.session: httpx.AsyncClient = None
        self.cookie = os.getenv('UNIVER_COOKIE') or "_univer=JVNDIQSSGB4TSUTHI5DHAQKCNJNFCMDUGZ3TE"

    async def initialize(self):
        """Initialize HTTP session"""
        # Configure connection limits (equivalent to aiohttp.TCPConnector)
        limits = httpx.Limits(
            max_keepalive_connections=200,  # Maximum keep-alive connections
            max_connections=200,  # Maximum connections
            keepalive_expiry=300.0  # Keep-alive expiry in seconds
        )

        # Configure timeout
        timeout = httpx.Timeout(
            timeout=600.0,  # Total timeout: 10 minutes
            connect=60.0,   # Connection timeout: 1 minute
            read=600.0      # Read timeout: 10 minutes
        )

        self.session = httpx.AsyncClient(
            limits=limits,
            timeout=timeout
        )

        print(f"🌐 HTTP session initialized (connection pool: {limits.max_connections})")

    async def close(self):
        """Close session"""
        if self.session:
            await self.session.close()
            print(f"🌐 HTTP session closed")

    def _parse_sse_event(self, event_text: str) -> Dict[str, Any]:
        """Parse a single SSE event into a dictionary

        Args:
            event_text: Raw SSE event text (e.g., "data: {...}" or ": ping - ...")

        Returns:
            dict: Parsed event data or None for ping events
        """
        event_text = event_text.strip()

        # Handle ping/keep-alive events
        if event_text.startswith(': ping'):
            return {'type': 'PING'}

        # Extract data content
        if event_text.startswith('data:'):
            data_content = event_text[5:].strip()  # Remove 'data:' prefix

            if data_content:
                try:
                    return json.loads(data_content)
                except json.JSONDecodeError as e:
                    return {'type': 'PARSE_ERROR', 'raw': data_content, 'error': str(e)}

        return {'type': 'UNKNOWN', 'raw': event_text}

    @retry(
        stop=stop_after_attempt(1),  # Retry up to 1 times
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

            # Construct form data for httpx
            files = {
                'file': (
                    os.path.basename(input_file_path),
                    file_content,
                    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                )
            }
            data = {
                'task': json.dumps(task_data),
            }

            # Send request with cookie
            headers = {
                'Cookie': self.cookie
            }
            # Use httpx.stream for SSE streaming (timeout: 10 minutes)
            async with self.session.stream(
                "POST",
                self.api_url,
                data=data,
                files=files,
                headers=headers,
                timeout=600.0  # 10 minutes
            ) as response:
                if response.status_code != 200:
                    # Try to get detailed error information from response body
                    error_details = f"Status code: {response.status_code}"
                    try:
                        # httpx response.text is a property, not a coroutine
                        response_text = response.text
                        if response_text:
                            # Try to parse as JSON
                            try:
                                error_json = json.loads(response_text)
                                error_details = f"Status code: {response.status_code}, Response: {json.dumps(error_json, ensure_ascii=False)}"
                            except json.JSONDecodeError:
                                # If not JSON, use text directly (truncate if too long)
                                if len(response_text) > 500:
                                    error_details = f"Status code: {response.status_code}, Response (truncated): {response_text[:500]}..."
                                else:
                                    error_details = f"Status code: {response.status_code}, Response: {response_text}"
                    except Exception as e:
                        # If we can't read response body, at least include status code
                        error_details = f"Status code: {response.status_code}, Failed to read response body: {str(e)}"
                    raise Exception(error_details)

                # SSE Stream Processing
                buffer = ""
                last_event = None

                async for chunk in response.aiter_text():
                    buffer += chunk

                    # Process complete events (split by \n\n)
                    while '\n\n' in buffer:
                        event_text, buffer = buffer.split('\n\n', 1)

                        if not event_text.strip():
                            continue

                        event_data = self._parse_sse_event(event_text)
                        event_type = event_data.get('type')

                        # Handle different event types
                        if event_type == 'RUN_STARTED':
                            # Task started, continue reading
                            pass

                        elif event_type == 'RUN_FINISHED':
                            # Extract fileUrl and return
                            result = event_data.get('result', {})
                            file_url = result.get('fileUrl')
                            if file_url:
                                return {
                                    "error": {"code": 1, "message": "success"},
                                    "fileUrl": file_url
                                }
                            else:
                                raise Exception(f"RUN_FINISHED event missing fileUrl: {json.dumps(event_data, ensure_ascii=False)}")

                        elif event_type == 'RUN_ERROR':
                            # Handle error event
                            error_msg = event_data.get('message', 'Unknown error from SSE stream')
                            error_code = event_data.get('code', 'UNKNOWN')
                            raise Exception(f"SSE Error (code: {error_code}): {error_msg}")

                        elif event_type == 'PING':
                            # Keep-alive ping, ignore
                            pass

                        elif event_type == 'PARSE_ERROR':
                            # Log parse errors but continue
                            pass

                        last_event = event_data

                # Stream ended without RUN_FINISHED
                if last_event:
                    raise Exception(f"SSE stream ended unexpectedly. Last event: {json.dumps(last_event, ensure_ascii=False)}")
                else:
                    raise Exception("SSE stream ended without any events")

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
            async with self.session.stream("GET", file_url) as response:
                if response.status_code == 200:
                    async with aiofiles.open(output_path, 'wb') as f:
                        async for chunk in response.aiter_bytes():
                            await f.write(chunk)
                    return True
                else:
                    raise Exception(f"Download failed: status {response.status_code}")
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

            # Call API (with auto retry)
            try:
                api_result = await self.bench_run(input_file_path, task)
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
    dataset_dir = Path(__file__).parent.parent / 'data' / opt.dataset
    dataset_json_path = dataset_dir / 'dataset.json'
    dataset = json.loads(dataset_json_path.read_text(encoding='utf-8'))

    # Create output folder
    output_folder = dataset_dir / 'outputs' / f'univer_{opt.model}'
    output_folder.mkdir(parents=True, exist_ok=True)
    print(f"📁 Output folder: {output_folder}")

    # Create log folder
    log_folder = dataset_dir / 'log'
    log_folder.mkdir(parents=True, exist_ok=True)

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
                task, dataset_dir, output_folder
            )

            # Update statistics
            if result['success']:
                stats['success'] += 1
            else:
                stats['failed'] += 1
                stats['errors'].append(result)

            log_file = log_folder / f'univer_{opt.model}.jsonl'
            try:
                async with aiofiles.open(log_file, 'a', encoding='utf-8') as f:
                    await f.write(json.dumps(result, ensure_ascii=False) + '\n')
            except Exception as e:
                print(f"⚠️  Warning: Failed to write log entry for task {result.get('id', 'unknown')}: {e}")

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
        await processor.session.aclose()

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
    print(f"   Log file: {log_folder}/univer_{opt.model}.jsonl")

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
    parser.add_argument('--mock', action='store_true', default=False,
                       help='Use mock API (default: False)')

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
